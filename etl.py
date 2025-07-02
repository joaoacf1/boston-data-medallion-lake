import os
import logging
import requests
import pandas as pd
import boto3
from dotenv import load_dotenv
from typing import Dict
from io import BytesIO

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

load_dotenv()

DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)

HEADERS = {'User-Agent': 'Mozilla/5.0'}

DATASETS = {
    "2025": "https://data.boston.gov/dataset/8048697b-ad64-4bfc-b090-ee00169f2323/resource/9d7c2214-4709-478a-a2e8-fb2020a5bb94/download/tmpxvr3p2yd.csv",
    "2024": "https://data.boston.gov/dataset/8048697b-ad64-4bfc-b090-ee00169f2323/resource/dff4d804-5031-443a-8409-8344efd0e5c8/download/tmpm461rr5o.csv",
    "2023": "https://data.boston.gov/dataset/8048697b-ad64-4bfc-b090-ee00169f2323/resource/e6013a93-1321-4f2a-bf91-8d8a02f1e62f/download/tmpwbgyud93.csv",
    "2022": "https://data.boston.gov/dataset/8048697b-ad64-4bfc-b090-ee00169f2323/resource/81a7b022-f8fc-4da5-80e4-b160058ca207/download/tmpfm8veglw.csv",
    "2021": "https://data.boston.gov/dataset/8048697b-ad64-4bfc-b090-ee00169f2323/resource/f53ebccd-bc61-49f9-83db-625f209c95f5/download/tmp88p9g82n.csv",
    "2020": "https://data.boston.gov/dataset/8048697b-ad64-4bfc-b090-ee00169f2323/resource/6ff6a6fd-3141-4440-a880-6f60a37fe789/download/tmpcv_10m2s.csv",
}

def extract_and_save_csv(year: str, url: str, output_dir: str = DATA_DIR) -> str:

    file_path = os.path.join(output_dir, f"data_{year}.csv")
    try:
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
        with open(file_path, 'wb') as f:
            f.write(response.content)
        logging.info(f"✓: {file_path}")
        return file_path
    except requests.RequestException as e:
        logging.error(f"Failed {url}: {e}")
        return ""


def read_csvs_to_dict(file_paths: Dict[str, str]) -> Dict[str, pd.DataFrame]:

    dfs = {}
    for year, path in file_paths.items():
        try:
            dfs[year] = pd.read_csv(path, low_memory=False)
        except Exception as e:
            logging.error(f"Failed {path}: {e}")
    return dfs


def upload_to_s3_parquet(dfs: Dict[str, pd.DataFrame], bucket: str, prefix: str = "bronze"):

    s3 = boto3.client('s3')
    
    for year, df in dfs.items():
        buffer = BytesIO()
        df.to_parquet(buffer, index=False)
        
        s3.put_object(
            Bucket=bucket,
            Key=f"{prefix}/data_{year}.parquet",
            Body=buffer.getvalue()
        )
        logging.info(f"✓ Upload to S3: {prefix}/data_{year}.parquet")

def main():

    required_env = ['AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'REGION_NAME']
    
    if not all(os.getenv(var) for var in required_env):
        raise EnvironmentError("AWS environment variables not set correctly.")

    boto3.setup_default_session(
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        region_name=os.getenv('REGION_NAME')
    )

    file_paths = {
        year: extract_and_save_csv(year, url)
        for year, url in DATASETS.items()
    }

    dfs = read_csvs_to_dict(file_paths)

    upload_to_s3_parquet(dfs, bucket="boston-data-lake")
    
if __name__ == "__main__":
    main()
