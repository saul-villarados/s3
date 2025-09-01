import aiohttp
import asyncio
import os
import aioboto3
from tqdm import tqdm
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from data import urls
from utils.logger import get_logger
from utils.custom_exception import CustomException
import time  # For backoff in retries

load_dotenv()

logger = get_logger(__name__)

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID") 
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")

async def download_images_async(max_concurrent=100, max_retries=3):
    failed_downloads = []  # Lista para almacenar (url, error)
    
    logger.info("Starting async image download and upload process")

    semaphore = asyncio.Semaphore(max_concurrent)
    timeout = aiohttp.ClientTimeout(total=30)  # Timeout de 30 segundos por request

    headers = {'User-Agent': 'ImageDownloader/1.0'}  # Añadir User-Agent para evitar posibles bloqueos

    async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
        try:
            logger.info("Setting up S3 client connection")
            s3_session = aioboto3.Session(
                aws_access_key_id=AWS_ACCESS_KEY_ID,
                aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                region_name=AWS_REGION
            )
            async with s3_session.client('s3') as s3_client:
                logger.info(f"Processing {len(urls[:25000])} URLs")
                
                tasks = []
                for i, img_url in enumerate(urls[:25000]):
                    task = asyncio.ensure_future(
                        download_and_upload_single(session, s3_client, img_url, i, semaphore, failed_downloads, max_retries)
                    )
                    tasks.append(task)
                
                # Ejecutar todas las tareas con progreso
                for f in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="Processing images"):
                    await f
                
        except Exception as e:
            logger.error(f"Failed to setup S3 client or process downloads: {str(e)}")
            raise CustomException("S3 client connection or download processing failed", e)
    
    try:
        # Guardar registro de fallos
        with open("failed_downloads.txt", "w") as f:
            for url, error in failed_downloads:
                f.write(f"{url}: {error}\n")
        
        logger.info(f"Download completed. Failures: {len(failed_downloads)}")
        print(f"Descarga completada. Fallos: {len(failed_downloads)}")
    except Exception as e:
        logger.error(f"Failed to save failed downloads log: {str(e)}")
        raise CustomException("Failed to save failed downloads log", e)

async def download_and_upload_single(session, s3_client, url, index, semaphore, failed_downloads, max_retries=3):
    async with semaphore:
        for attempt in range(max_retries):
            try:
                async with session.get(url) as response:
                    if response.status == 200:
                        content = await response.read()
                        # Extraer extensión
                        ext = get_extension_from_response(response)
                        filename = f"{index}{ext}"
                        
                        # Subir a S3
                        await s3_client.put_object(
                            Bucket=S3_BUCKET_NAME,
                            Key=f"images/{filename}",
                            Body=content,
                            ContentType=response.headers.get('Content-Type', 'image/jpeg')
                        )
                        
                        if index % 1000 == 0:  # Log cada 1000 para no sobrecargar
                            logger.info(f"Successfully uploaded image {index}")
                        return  # Éxito, salir
                    else:
                        err_msg = f"HTTP Status: {response.status}"
                        if attempt == max_retries - 1:
                            logger.warning(f"Failed to download {url} after {max_retries} attempts - {err_msg}")
                            failed_downloads.append((url, err_msg))
                        else:
                            await asyncio.sleep(2 ** attempt)  # Backoff
                            continue
            except aiohttp.ClientPayloadError as e:
                err_msg = f"PayloadError: {str(e)}"
                if attempt == max_retries - 1:
                    logger.error(f"Error processing {url} after {max_retries} attempts: {err_msg}")
                    failed_downloads.append((url, err_msg))
                else:
                    logger.warning(f"Retrying {url} due to {err_msg} (attempt {attempt+1})")
                    await asyncio.sleep(2 ** attempt)  # Backoff
                    continue
            except aiohttp.ClientError as e:
                err_msg = f"ClientError: {str(e)}"
                if attempt == max_retries - 1:
                    logger.error(f"Error processing {url} after {max_retries} attempts: {err_msg}")
                    failed_downloads.append((url, err_msg))
                else:
                    logger.warning(f"Retrying {url} due to {err_msg} (attempt {attempt+1})")
                    await asyncio.sleep(2 ** attempt)
                    continue
            except ClientError as e:
                err_code = e.response['Error']['Code'] if 'Error' in e.response else 'Unknown'
                err_msg = f"S3 ClientError ({err_code}): {str(e)}"
                if err_code == 'IncompleteBody' and attempt < max_retries - 1:
                    logger.warning(f"Retrying {url} due to {err_msg} (attempt {attempt+1})")
                    await asyncio.sleep(2 ** attempt)
                    continue
                else:
                    logger.error(f"Error uploading for {url} after {max_retries} attempts: {err_msg}")
                    failed_downloads.append((url, err_msg))
            except Exception as e:
                err_msg = f"Unexpected ({type(e).__name__}): {str(e)}"
                if attempt == max_retries - 1:
                    logger.error(f"Error processing {url} after {max_retries} attempts: {err_msg}")
                    failed_downloads.append((url, err_msg))
                else:
                    logger.warning(f"Retrying {url} due to {err_msg} (attempt {attempt+1})")
                    await asyncio.sleep(2 ** attempt)
                    continue

def get_extension_from_response(response):
    content_type = response.headers.get('Content-Type', '').lower()
    if 'jpeg' in content_type or 'jpg' in content_type:
        return '.jpg'
    elif 'png' in content_type:
        return '.png'
    else:
        # Si no se puede determinar, intentar extraer de la URL
        return os.path.splitext(str(response.url))[1] or '.jpg'

# Ejecutar
asyncio.run(download_images_async())