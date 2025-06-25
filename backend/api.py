import os
import logging
from fastapi import FastAPI
from .vector_models import Vector
from .vector_tables import DBManager

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
 
DB_FILE_NAME = "my_vector_database.db"

app = FastAPI()
db_manager = DBManager(DB_FILE_NAME)

