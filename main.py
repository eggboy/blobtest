import concurrent
from itertools import islice
from multiprocessing import Pool

from azure.storage.blob import BlobServiceClient
from timeit import default_timer as timer
from environs import Env
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)

env = Env()

with env.prefixed("DOWNLOADER_"):
    imputCSV = env("INPUT_CSV", "file_list.csv")
    Workers = env.int("WORKERS", 5)
    Rounds = env.int("ROUNDS", 1)
    DiscardHeader = 1 if env.bool("DISCARD_HEADER", True) == True else 0
    ConnectionString = env("CONNECTION_STRING", "")

storageAccount = "microsoft"
print(f"CONNECTION_STRING : {ConnectionString}")
blob_service_client = BlobServiceClient.from_connection_string(ConnectionString)
container_client = blob_service_client.get_container_client(storageAccount)

def download_image(filePath):
    # filePath = filePath.replace("azure:", "")
    # filePathSplit = filePath.split("/", 1)
    storageAccount = "microsoft"
    # path = filePathSplit[1]
    path = filePath
    blob_client = container_client.get_blob_client(path)

    start = timer()
    #logger.info("Downloading " + path)
    download_stream = blob_client.download_blob()
    logger.info(str(download_stream.size))
    end = timer()
    elapsed_time = (end - start)
    logger.info("Elapsed Time : " + str(elapsed_time))

    return download_stream.size

if __name__ == "__main__":
    futures = []
    logger.info("Starting...")

    fileList = open(imputCSV, "r").readlines()
    list = []
    for file in islice(fileList, DiscardHeader, None):
        file = file.replace("\n", "").replace("\r", "")
        list.append(file)
    start = timer()
    with Pool(Workers) as p:
        mbs = p.map(download_image, list)
    p.close()
    p.join()
    end = timer()
    elapsed_time = (end - start)
    totalBytes = 0

    for i in mbs:
        totalBytes += i
    print(f"totalBytes : {totalBytes}, totalSeconds : {elapsed_time}")
