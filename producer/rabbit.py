import aio_pika
import json
import logging
from models import StatusUpdate, ScreenshotTask
import asyncio
from logger_config import setup_logger

logger = setup_logger("rabbit-producer")

class RabbitMQ:
    def __init__(self):
        self.connection = None
        self.channel = None
    
    async def connect(self, url: str, max_retries: int = 10, delay: float = 5.0):
        for attempt in range(max_retries):
            try:
                self.connection = await aio_pika.connect_robust(url)
                self.channel = await self.connection.channel()
                
                await self.channel.declare_queue("screenshot_jobs", durable=True)
                await self.channel.declare_queue("storage_jobs", durable=True)
                await self.channel.declare_queue("status_updates", durable=True)
                
                logger.info("Подключение к RabbitMQ успешно установлено")
                return self
                
            except Exception as e:
                logger.warning(f"Ошибка подключения к RabbitMQ (попытка {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(delay)
                else:
                    logger.error(f"Не удалось подключиться к RabbitMQ после {max_retries} попыток")
                    raise
    
    async def publish_screenshot_task(self, task_data: dict):
        try:
            ScreenshotTask(**task_data)
            
            await self.channel.default_exchange.publish(
                aio_pika.Message(
                    body=json.dumps(task_data).encode(),
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                    content_type="application/json"
                ),
                routing_key="screenshot_jobs"
            )
            logger.info(f"Опубликована задача скриншота: {task_data.get('job_id')}")
        except Exception as e:
            logger.error(f"Ошибка публикации задачи скриншота: {e}")
            raise
    
    async def publish_status_update(self, status_update: StatusUpdate):
        try:
            message_body = status_update.json()
            
            await self.channel.default_exchange.publish(
                aio_pika.Message(
                    body=message_body.encode(),
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                    content_type="application/json",
                    headers={"job_id": str(status_update.job_id)}
                ),
                routing_key="status_updates"
            )
            logger.info(f"Опубликован статус {status_update.status} для задачи {status_update.job_id}")
        except Exception as e:
            logger.error(f"Ошибка публикации статуса: {e}")
            raise
    
    async def consume_status_updates(self, callback):
        try:
            queue = await self.channel.declare_queue("status_updates", durable=True)
            
            logger.info("Запуск потребителя обновлений статусов...")
            
            async for message in queue:
                try:
                    async with message.process():
                        body = message.body.decode()
                        status_data = json.loads(body)
                        
                        status_update = StatusUpdate(**status_data)
                        
                        await callback(status_update)
                        
                        logger.info(f"Обработано обновление статуса для задачи: {status_update.job_id} - {status_update.status}")
                        
                except json.JSONDecodeError as e:
                    logger.error(f"Ошибка декодирования JSON в сообщении статуса: {e}")
                except Exception as e:
                    logger.error(f"Ошибка обработки сообщения статуса: {e}")
                    
        except Exception as e:
            logger.error(f"Ошибка в потребителе обновлений статусов: {e}")
            raise
    
    async def close(self):
        if self.connection:
            await self.connection.close()
            logger.info("Соединение с RabbitMQ закрыто")