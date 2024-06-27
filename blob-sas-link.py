from azure.storage.blob import BlobServiceClient, generate_blob_sas, BlobSasPermissions
from datetime import datetime, timedelta

# Data for Connection Purposes
connection_string = "my connection string goes here"
container_name = "containers"
blob_name = "extracted_data.csv"

blob_service_client = BlobServiceClient.from_connection_string(connection_string)

# SAS URL Creation
# 
sas_token = generate_blob_sas(
    account_name=blob_service_client.account_name,
    container_name=container_name,
    blob_name=blob_name,
    account_key=blob_service_client.credential.account_key,
    permission=BlobSasPermissions(read=True),
    expiry=datetime.utcnow() + timedelta(hours=1)  # Adjust expiry time as needed
)

sas_url = f"https://{blob_service_client.account_name}.blob.core.windows.net/{container_name}/{blob_name}?{sas_token}"
print(sas_url)
