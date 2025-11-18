import asyncio
import logging
import aio_pika
from playwright.async_api import async_playwright
import base64
from rabbit import RabbitMQ
from models import ScreenshotTask, StatusUpdate, StorageTask, JobStatus
import json
from logger_config import setup_logger

logger = setup_logger("screenshot-worker")

class ScreenshotWorker:
    def __init__(self):
        self.rabbit = RabbitMQ()
        self.browser = None
        self.context = None
    
    async def init_browser(self):
        playwright = await async_playwright().start()
        self.browser = await playwright.chromium.launch(
            args=['--no-sandbox', '--disable-dev-shm-usage']
        )
        self.context = await self.browser.new_context(
            viewport={'width': 1920, 'height': 1080}
        )
    
    async def take_screenshot(self, task: ScreenshotTask):
        try:
            page = await self.context.new_page()
            
            await page.goto(task.url, wait_until='networkidle')
            
            if task.selector:
                element = await page.wait_for_selector(task.selector)
                screenshot_bytes = await element.screenshot(type='png')
            else:
                screenshot_bytes = await page.screenshot(full_page=True, type='png')
            
            await page.close()
            
            base64_image = base64.b64encode(screenshot_bytes).decode('utf-8')
            return base64_image
            
        except Exception as e:
            logger.error(f"Ошибка при создании скриншота для {task.job_id}: {e}")
            raise
    
    async def process_screenshot_task(self, task: ScreenshotTask, rabbit: RabbitMQ):
        try:
            logger.info(f"Обработка задачи скриншота: {task.job_id}")
            
            capturing_status = StatusUpdate(
                job_id=task.job_id,
                status=JobStatus.CAPTURING,
                message="Захват скриншота"
            )
            await rabbit.publish_status_update(capturing_status)
            
            base64_image = await self.take_screenshot(task)
            
            uploading_status = StatusUpdate(
                job_id=task.job_id,
                status=JobStatus.UPLOADING,
                message="Подготовка к сохранению"
            )
            await rabbit.publish_status_update(uploading_status)
            
            storage_task = StorageTask(
                job_id=task.job_id,
                base64_image=base64_image
            )
            
            await rabbit.channel.default_exchange.publish(
                aio_pika.Message(
                    body=storage_task.model_dump_json().encode(),
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                    content_type="application/json"
                ),
                routing_key="storage_jobs"
            )
            
            logger.info(f"Скриншот создан и отправлен на сохранение: {task.job_id}")
            
        except Exception as e:
            logger.error(f"Ошибка обработки задачи {task.job_id}: {e}")
            
            failed_status = StatusUpdate(
                job_id=task.job_id,
                status=JobStatus.FAILED,
                message=f"Ошибка создания скриншота: {str(e)}"
            )
            await rabbit.publish_status_update(failed_status)
    
    async def consume_screenshot_tasks(self):
        queue = await self.rabbit.channel.declare_queue("screenshot_jobs", durable=True)
        
        logger.info("Запуск потребителя задач скриншотов...")
        
        async for message in queue:
            try:
                body = message.body.decode()
                task_data = json.loads(body)
                task = ScreenshotTask(**task_data)
                
                await self.process_screenshot_task(task, self.rabbit)
                await message.ack()
                
            except Exception as e:
                logger.error(f"Ошибка обработки сообщения скриншота: {e}")
                await message.nack(requeue=False)

async def main():
    worker = ScreenshotWorker()
    
    try:
        await worker.rabbit.connect("amqp://guest:guest@rabbitmq:5672/")
        await worker.init_browser()
        
        logger.info("Screenshot worker успешно запущен")
        
        await worker.consume_screenshot_tasks()
        
    except Exception as e:
        logger.error(f"Ошибка запуска screenshot worker: {e}")
    finally:
        if worker.browser:
            await worker.browser.close()

if __name__ == "__main__":
    asyncio.run(main())