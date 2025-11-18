import aio_pika
import json
import logging
from models import StorageTask, StatusUpdate, JobStatus
import asyncio
from logger_config import setup_logger

logger = setup_logger("storage-rabbit"))

class RabbitMQ:
    def __init__(self):
        self.connection = None
        self.channel = None
    
    async def connect(self, url: str, max_retries: int = 30, delay: float = 2.0):
        for attempt in range(max_retries):
            try:
                self.connection = await aio_pika.connect_robust(url)
                self.channel = await self.connection.channel()
                
                await self.channel.declare_queue("storage_jobs", durable=True)
                await self.channel.declare_queue("status_updates", durable=True)
                
                logger.info("Успешное подключение к RabbitMQ")
                return self
                
            except Exception as e:
                logger.warning(f"Попытка {attempt + 1}/{max_retries}: Ошибка подключения к RabbitMQ - {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(delay)
                else:
                    logger.error(f"Не удалось подключиться к RabbitMQ после {max_retries} попыток")
                    raise
    
    async def consume_storage_tasks(self, callback):
        try:
            queue = await self.channel.declare_queue("storage_jobs", durable=True)
            
            logger.info("Запуск потребителя задач хранения...")
            
            async for message in queue:
                try:
                    body = message.body.decode()
                    task_data = json.loads(body)
                    task = StorageTask(**task_data)
                    
                    await callback(task, self)
                    await message.ack()
                    logger.info(f"Обработана задача хранения: {task.job_id}")
                    
                except Exception as e:
                    logger.error(f"Ошибка обработки сообщения хранения: {e}")
        except Exception as e:
            logger.error(f"Ошибка в потребителе задач хранения: {e}")
            raise
    
    async def publish_status_update(self, status_update: StatusUpdate):
        try:
            message_body = status_update.json()
            
            await self.channel.default_exchange.publish(
                aio_pika.Message(
                    body=message_body.encode(),
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                    content_type="application/json"
                ),
                routing_key="status_updates"
            )
            logger.info(f"Опубликован статус: {status_update.status} для задачи {status_update.job_id}")
        except Exception as e:
            logger.error(f"Ошибка публикации статуса: {e}")
            raise