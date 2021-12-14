import os
from azure.identity import DefaultAzureCredential

# Import the client object from the SDK library
from azure.storage.blob import BlobClient

credential = DefaultAzureCredential()

# Retrieve the storage blob service URL, which is of the form
# https://lizlogan.blob.core.windows.net/
storage_url = "https://lizlogan.blob.core.windows.net"
# storage_url = os.environ["STORAGE_BLOB_URL"]

# Create the client object using the storage URL and the credential
blob_client = BlobClient(storage_url,
    container_name="pop-win", blob_name="sample-blob.txt", credential=credential)

# Open a local file and upload its contents to Blob Storage
with open("../src/sample-source.txt", "rb") as data:
    blob_client.upload_blob(data)