import os
from dotenv import load_dotenv

load_dotenv()

API_TOKEN = os.getenv("API_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")

if not API_TOKEN or not DATABASE_URL:
    raise ValueError("Missing API_TOKEN or DATABASE_URL!")