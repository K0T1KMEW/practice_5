from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
import uuid
import asyncio
import json
import logging
from contextlib import asynccontextmanager
from logger_config import setup_logger


from rabbit import RabbitMQ
from models import ScreenshotRequest, StatusUpdate, JobStatus, DefaultResponse

logger = setup_logger("producer")

class ConnectionManager:
    def __init__(self):
        self.active_connections = {}
    
    async def connect(self, websocket: WebSocket, job_id: str):
        await websocket.accept()
        if job_id not in self.active_connections:
            self.active_connections[job_id] = set()
        self.active_connections[job_id].add(websocket)
        logger.info(f"WebSocket подключен для задачи {job_id}. Всего соединений: {len(self.active_connections[job_id])}")
    
    def disconnect(self, websocket: WebSocket, job_id: str):
        if job_id in self.active_connections:
            self.active_connections[job_id].discard(websocket)
            if not self.active_connections[job_id]:
                del self.active_connections[job_id]
            logger.info(f"WebSocket отключен для задачи {job_id}")
    
    async def broadcast_to_job(self, job_id: str, message: str):
        job_id_str = str(job_id)
        if job_id_str in self.active_connections:
            disconnected = []
            for websocket in self.active_connections[job_id_str].copy():
                try:
                    await websocket.send_text(message)
                    logger.debug(f"Сообщение отправлено через WebSocket для задачи {job_id_str}")
                except Exception as e:
                    logger.error(f"Ошибка отправки через WebSocket: {e}")
                    disconnected.append(websocket)
            
            for websocket in disconnected:
                self.disconnect(websocket, job_id_str)
    
    async def broadcast_status(self, status_update: StatusUpdate):
        message = status_update.model_dump_json()
        await self.broadcast_to_job(status_update.job_id, message)
        logger.info(f"Отправлен статус {status_update.status} для задачи {status_update.job_id}")
    
    def get_connection_count(self, job_id: str):
        return len(self.active_connections.get(job_id, []))

manager = ConnectionManager()
rabbit = RabbitMQ()

async def handle_status_update(status_update: StatusUpdate):
    try:
        logger.info(f"Получено обновление статуса для задачи {status_update.job_id}: {status_update.status} - {status_update.message}")
        
        connection_count = manager.get_connection_count(str(status_update.job_id))
        logger.info(f"Активных WebSocket соединений для задачи {status_update.job_id}: {connection_count}")
        
        await manager.broadcast_status(status_update)
        logger.info(f"Статус {status_update.status} отправлен через WebSocket для задачи {status_update.job_id}")
    except Exception as e:
        logger.error(f"Ошибка обработки обновления статуса: {e}")

async def start_status_consumer():
    try:
        logger.info("Запуск потребителя обновлений статусов...")
        await rabbit.consume_status_updates(handle_status_update)
    except Exception as e:
        logger.error(f"Ошибка потребителя статусов: {e}")

@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        await rabbit.connect("amqp://guest:guest@rabbitmq:5672/")
        logger.info("Сервис Producer успешно запущен")
        
        asyncio.create_task(start_status_consumer())
        logger.info("Потребитель обновлений статусов запущен")
        
        yield
        
    except Exception as e:
        logger.error(f"Ошибка запуска сервиса Producer: {e}")
        raise
    finally:
        if rabbit.connection:
            await rabbit.connection.close()

app = FastAPI(lifespan=lifespan)
templates = Jinja2Templates(directory="templates")

@app.get("/", response_class=HTMLResponse)
async def read_root(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.post("/screenshot")
async def create_screenshot(request: ScreenshotRequest):
    try:
        job_id = uuid.uuid4()
        
        screenshot_task = {
            "job_id": str(job_id),
            "url": request.url,
            "selector": request.selector
        }
        
        await rabbit.publish_screenshot_task(screenshot_task)
        
        status_update = StatusUpdate(
            job_id=job_id,
            status=JobStatus.PENDING,
            message="Задача создана и отправлена в очередь"
        )
        await rabbit.publish_status_update(status_update)
        
        await manager.broadcast_status(status_update)
        
        logger.info(f"Создана задача скриншота: {job_id} для URL: {request.url}")
        
        return DefaultResponse(
            error=False,
            message="Задача создана успешно",
            payload={
                "job_id": job_id, 
                "status": "pending"
            }
        )
    
    except Exception as e:
        logger.error(f"Ошибка создания задачи скриншота: {e}")
        return DefaultResponse(
            error=True,
            message=f"Ошибка создания задачи: {str(e)}"
        )

@app.websocket("/ws/status/{job_id}")
async def websocket_endpoint(websocket: WebSocket, job_id: str):
    await manager.connect(websocket, job_id)
    
    try:
        welcome_message = StatusUpdate(
            job_id=uuid.UUID(job_id),
            status=JobStatus.PENDING,
            message="Подключение к отслеживанию статуса установлено"
        )
        await websocket.send_text(welcome_message.model_dump_json())
        
        while True:
            data = await websocket.receive_text()
            if data == "ping":
                await websocket.send_text(json.dumps({"type": "pong", "timestamp": asyncio.get_event_loop().time()}))
            else:
                logger.debug(f"Получено сообщение от клиента {job_id}: {data}")
                
    except WebSocketDisconnect:
        logger.info(f"WebSocket клиент отключен для задачи {job_id}")
        manager.disconnect(websocket, job_id)
    except Exception as e:
        logger.error(f"Ошибка WebSocket для задачи {job_id}: {e}")
        manager.disconnect(websocket, job_id)

@app.get("/ws/stats/{job_id}")
async def get_websocket_stats(job_id: str):
    return DefaultResponse(
        error=False,
        message="Статистика WebSocket",
        payload={
            "job_id": job_id,
            "active_connections": manager.get_connection_count(job_id)
        }
    )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)