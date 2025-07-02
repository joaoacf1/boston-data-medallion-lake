import os
import logging
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

load_dotenv()

DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)