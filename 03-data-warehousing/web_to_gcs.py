import os
import requests
import pandas as pd
from google.cloud import storage
import time

# Configuration
BUCKET_NAME = os.environ.get("GCP_GCS_BUCKET", "green_taxi_gcs_1")
# for yellow taxi data, change the bucket name to "yellow_taxi_gcs"
# for green taxi data, change the bucket name to "green_taxi_gcs_1"
CREDENTIALS_FILE = "gcs.json"  
DOWNLOAD_DIR = "."
CHUNK_SIZE = 5 * 1024 * 1024  # 5 MB chunk size
BASE_URL = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/'

# Create download directory
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# Initialize GCS client
client = storage.Client.from_service_account_json(CREDENTIALS_FILE)
bucket = client.bucket(BUCKET_NAME)

def download_file(year, month, service):
    """Downloads a single file from the source URL"""
    file_name = f"{service}_tripdata_{year}-{month:02d}.csv.gz"
    request_url = f"{BASE_URL}{service}/{file_name}"
    file_path = os.path.join(DOWNLOAD_DIR, file_name)

    try:
        print(f"Downloading {request_url}...")
        response = requests.get(request_url)
        with open(file_path, 'wb') as f:
            f.write(response.content)
        print(f"Downloaded: {file_path}")
        return file_path
    except Exception as e:
        print(f"Failed to download {request_url}: {e}")
        return None

def convert_to_parquet(file_path):
    """Converts CSV to Parquet format"""
    try:
        df = pd.read_csv(file_path, compression='gzip')
        parquet_path = file_path.replace('.csv.gz', '.parquet')
        df.to_parquet(parquet_path, engine='pyarrow')
        print(f"Converted to Parquet: {parquet_path}")
        return parquet_path
    except Exception as e:
        print(f"Failed to convert {file_path}: {e}")
        return None

def verify_gcs_upload(blob_name):
    """Verifies if file exists in GCS"""
    return storage.Blob(bucket=bucket, name=blob_name).exists(client)

def upload_to_gcs(file_path, service, max_retries=3):
    """Uploads file to GCS with retry logic"""
    blob_name = f"{service}/{os.path.basename(file_path)}"
    blob = bucket.blob(blob_name)
    blob.chunk_size = CHUNK_SIZE

    for attempt in range(max_retries):
        try:
            print(f"Uploading {file_path} to {BUCKET_NAME} (Attempt {attempt + 1})...")
            blob.upload_from_filename(file_path)
            print(f"Uploaded: gs://{BUCKET_NAME}/{blob_name}")
            
            if verify_gcs_upload(blob_name):
                print(f"Verification successful for {blob_name}")
                return True
            else:
                print(f"Verification failed for {blob_name}, retrying...")
        except Exception as e:
            print(f"Failed to upload {file_path} to GCS: {e}")
        
        time.sleep(5)
    
    print(f"Giving up on {file_path} after {max_retries} attempts.")
    return False

def cleanup_local_files(file_paths):
    """Removes downloaded and converted files"""
    for file_path in file_paths:
        if file_path and os.path.exists(file_path):
            os.remove(file_path)
            print(f"Cleaned up local file: {file_path}")

def check_bucket_exists():
    """Verifies if GCS bucket exists"""
    try:
        client.get_bucket(BUCKET_NAME)
        return True
    except Exception as e:
        print(f"Error accessing bucket {BUCKET_NAME}: {e}")
        return False

def process_taxi_data(year, service):
    """Main processing function for taxi data"""
    if not check_bucket_exists():
        print("Exiting due to bucket access issues")
        return False

    for month in range(1, 13):
        # Download
        csv_file = download_file(year, month, service)
        if not csv_file:
            continue

        # Convert
        parquet_file = convert_to_parquet(csv_file)
        if not parquet_file:
            cleanup_local_files([csv_file])
            continue

        # Upload
        upload_success = upload_to_gcs(parquet_file, service)
        
        # Cleanup
        cleanup_local_files([csv_file, parquet_file])

        if not upload_success:
            print(f"Failed to process {year}-{month:02d} for {service}")

if __name__ == "__main__":
    process_taxi_data('2019', 'green')
    #process_taxi_data('2020', 'green')
    # process_taxi_data('2019', 'yellow')
    # process_taxi_data('2020', 'yellow')
    print("All files processed, verified, and cleaned up.")
