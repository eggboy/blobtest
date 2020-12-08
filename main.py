from azure.storage.blob import BlobServiceClient
import os
from timeit import default_timer as timer
from flask import Flask
from flask_restful import Resource, Api
from multiprocessing import Pool

app = Flask(__name__)
api = Api(app)

connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")

parallism = 5

@app.route('/file/<filename>')
def home(filename):
    images_list = []
    i = 0

    while i <= parallism:
        i += 1
        images_list.append(filename)

    with Pool(parallism) as p:
        mbs = p.map(download_image, images_list)

    total = 0
    for i in mbs:
        total += i
    return str(total)


def download_image(filename):
    print("Downloading " + filename)

    blob_service_client = BlobServiceClient.from_connection_string(
        connection_string)
    container_client = blob_service_client.get_container_client("grabtest")
    blob_client = container_client.get_blob_client(filename)

    start = timer()
    download_stream = blob_client.download_blob()
    print(str(download_stream.size))
    end = timer()
    print("Elapsed Time : " + str(end - start))

    return (download_stream.size/1024/1024) / (end - start)
    # print("Elapsed Time : " + str(end - start) + " Download Size : " + str(download_stream.size / 1024 / 1024))


if __name__ == '__main__':
    app.run(debug=True)
