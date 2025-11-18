import asyncio
import logging
import base64
from miniopy_async import Minio
from rabbit import RabbitMQ
from models import StorageTask, StatusUpdate, JobStatus
import io
from logger_config import setup_logger

logger = setup_logger("storage-worker")

class MinIOClient:
    def __init__(self):
        self.client = None
    
    async def connect(self, endpoint: str, access_key: str, secret_key: str, max_retries: int = 30, delay: float = 2.0):
        for attempt in range(max_retries):
            try:
                self.client = Minio(
                    endpoint,
                    access_key=access_key,
                    secret_key=secret_key,
                    secure=False
                )
                
                await self.client.list_buckets()
                
                if not await self.client.bucket_exists("screenshots"):
                    await self.client.make_bucket("screenshots")
                    logger.info("Создан бакет 'screenshots'")
                
                logger.info("Успешное подключение к MinIO")
                return self
                
            except Exception as e:
                logger.warning(f"Попытка {attempt + 1}/{max_retries}: Ошибка подключения к MinIO - {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(delay)
                else:
                    logger.error(f"Не удалось подключиться к MinIO после {max_retries} попыток")
                    raise

    async def upload_image(self, job_id: str, base64_image: str):
        try:
            image_bytes = base64.b64decode(base64_image)
            
            image_stream = io.BytesIO(image_bytes)
            
            object_name = f"{job_id}.png"
            
            await self.client.put_object(
                "screenshots",
                object_name,
                data=image_stream,
                length=len(image_bytes),
                content_type="image/png"
            )
            
            logger.info(f"Успешная загрузка изображения: {object_name}")
            return f"screenshots/{object_name}"
            
        except Exception as e:
            logger.error(f"Ошибка загрузки изображения: {e}")
            raise

async def process_storage_task(task: StorageTask, rabbit: RabbitMQ, minio_client: MinIOClient):
    try:
        logger.info(f"Обработка задачи хранения: {task.job_id}")
        
        image_path = await minio_client.upload_image(str(task.job_id), task.base64_image)
        
        completed_status = StatusUpdate(
            job_id=task.job_id,
            status=JobStatus.COMPLETED,
            message="Скриншот успешно сохранен",
            image_url=image_path
        )
        await rabbit.publish_status_update(completed_status)
        logger.info(f"Хранение завершено для задачи: {task.job_id}")
        
    except Exception as e:
        logger.error(f"Ошибка обработки задачи хранения {task.job_id}: {e}")
        
        failed_status = StatusUpdate(
            job_id=task.job_id,
            status=JobStatus.FAILED,
            message=f"Ошибка сохранения: {str(e)}"
        )
        await rabbit.publish_status_update(failed_status)

async def main():
    rabbit = RabbitMQ()
    minio_client = MinIOClient()
    
    try:
        await rabbit.connect("amqp://guest:guest@rabbitmq:5672/")
        
        await minio_client.connect(
            endpoint="minio:9000",
            access_key="minioadmin", 
            secret_key="minioadmin"
        )
        
        logger.info("Сервис хранения успешно запущен")
        
        await rabbit.consume_storage_tasks(
            lambda task, rabbit: process_storage_task(task, rabbit, minio_client)
        )
        
    except Exception as e:
        logger.error(f"Ошибка запуска сервиса хранения: {e}")

if __name__ == "__main__":
    asyncio.run(main())