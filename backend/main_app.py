import os
import logging
import random
import datetime
import time

from vector_models import Vector, generate_one_vector
from backend.database_manager import DBManager

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

DB_FILE_NAME = "my_vector_database.db"

if __name__ == "__main__":
    logging.info("Starting vector Demo")

    db_manager = DBManager(DB_FILE_NAME)
    db_manager.create_tables()

    
    
   
            
            
                                              

       



