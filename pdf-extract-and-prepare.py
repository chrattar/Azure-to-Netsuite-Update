from azure.storage.blob import BlobServiceClient, generate_blob_sas, BlobSasPermissions
from datetime import datetime, timedelta
import pymupdf  # PyMuPDF
import re
import csv
import os

# AZ-Blob Connection Info
connection_string = "connectionstring"
container_name = "mynicecontainer"

# Initialize BlobServiceClient to Pull
blob_service_client = BlobServiceClient.from_connection_string(connection_string)
container_client = blob_service_client.get_container_client(container_name)

def list_blobs(container_client):
    blob_list = container_client.list_blobs()
    return [blob.name for blob in blob_list]

def generate_sas_url(blob_name):
    sas_token = generate_blob_sas(
        account_name=blob_service_client.account_name,
        container_name=container_name,
        blob_name=blob_name,
        account_key=blob_service_client.credential.account_key,
        permission=BlobSasPermissions(read=True),
        expiry=datetime.utcnow() + timedelta(hours=1)  # Adjust expiry time as needed
    )
    return f"https://{blob_service_client.account_name}.blob.core.windows.net/{container_name}/{blob_name}?{sas_token}"

def extract_text_from_pdf(pdf_file):
    try:
        doc = pymupdf.open(pdf_file)
        text = ""
        for page_num in range(len(doc)):
            page = doc.load_page(page_num)
            text += page.get_text()
        doc.close()
        return text
    except Exception as e:
        print(f"Error extracting text from {pdf_file}: {str(e)}")
        return None

def parse_text(text, vendor_patterns):
    extracted_data = {}

    for key, pattern in vendor_patterns.items():
        match = re.search(pattern, text)
        if match:
            extracted_data[key] = match.group(1)

    return extracted_data

def append_to_csv(data, csv_file):
    file_exists = os.path.isfile(csv_file)
    
    try:
        with open(csv_file, mode='a', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=data.keys())
            if not file_exists:
                writer.writeheader()  # File doesn't exist yet, write a header
            writer.writerow(data)
        print(f"Data successfully appended to {csv_file}")
    except Exception as e:
        print(f"Error appending data to {csv_file}: {str(e)}")

# Blocks of required fields for the CSV Upload
# We are really only looking to update Vendor Ship Dates and associate them to PO.
# This can be generic based on any type of text field that you are looking for in any document
# These are set to basic lines for invoices or shipping notifications
vendor_patterns = {
    'Vendor2': {
        'Order Date': r'Order Date:\s*(\S+)',
        'Order No': r'Order No:\s*(\S+)',
        'Order Amount': r'Order Amount:\s*\$([\d,]+\.\d{2})',
        'Customer PO': r'Customer PO:\s*(\S+)',
        'Product': r'Description\s*(\S+)'
    },
    'Vendor1': {
        'Order Date': r'Order Date:\s*(\S+)',
        'Order No': r'Order No:\s*(\S+)',
        'Order Amount': r'Order Amount:\s*\$([\d,]+\.\d{2})',
        'Customer PO': r'Customer PO:\s*(\S+)',
        'Product': r'Description\s*(\S+)'
    }
    # Add patterns for other vendors as needed
}

def identify_vendor(file_name):
    if 'Vendor2' in file_name:
        return 'Vendor2'
    elif 'Vendor1' in file_name:
        return 'Vendor1'
    # Add conditions for other vendors as needed
    else:
        return None

# List all blobs in the container
blobs = list_blobs(container_client)

# CSV file to store the extracted data
csv_file = "extracted_data.csv"

for blob_name in blobs:
    sas_url = generate_sas_url(blob_name)
    # Download contents of blob with URL
    try:
        # dl blob data to a local file
        local_pdf_file = blob_name.split('/')[-1]  # USe blob name as the local filename
        with open(local_pdf_file, 'wb') as file:
            file.write(container_client.download_blob(blob_name).readall())
        
        # Identify the document source (vendor, cutomer, etc for any document source) and process
        vendor = identify_vendor(blob_name)
        if vendor:
            extracted_text = extract_text_from_pdf(local_pdf_file)
            if extracted_text:
                parsed_data = parse_text(extracted_text, vendor_patterns[vendor])
                print("\nParsed Data for {}:".format(vendor))
                for key, value in parsed_data.items():
                    print(f"{key}: {value}")
                append_to_csv(parsed_data, csv_file)
            else:
                print(f"Text extraction failed for {blob_name}")
        else:
            print(f"Vendor not identified for {blob_name}")
        
        # Optionally, delete the local file after processing
        # Another option is to move to archive as required to free up room on the email server.
        os.remove(local_pdf_file)
    except Exception as e:
        print(f"Error processing {blob_name}: {str(e)}")
