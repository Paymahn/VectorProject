print("--- DEBUG: main_app.py is being executed ---")

import os
import logging
import random
import datetime
import time

from vector_models import Vector, generate_one_vector
from vector_tables import DBManager

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

DB_FILE_NAME = "my_vector_database.db"

if __name__ == "__main__":
    logging.info("Starting vector Demo")

    if os.path.exists(DB_FILE_NAME):
        os.remove(DB_FILE_NAME)
        logging.info("Deleted old DB")

    db_manager = DBManager(DB_FILE_NAME)
    db_manager.create_tables()

    logging.info("Created new DB tables")
    inserted_vector_ids = []
    
    
    logging.info("Creating 10 vectors")
    for i in range(10):
        new_vector = generate_one_vector() # This is at one level of indentation
        vector_id_from_db = db_manager.insert_vector_data(new_vector)

        if vector_id_from_db is not None:
            new_vector.id = vector_id_from_db
            inserted_vector_ids.append(vector_id_from_db)
            logging.info(f"Added a new vector with id: {vector_id_from_db}")
        time.sleep(0.05) # <--- Make sure this line's indentation matches 'new_vector = generate_one_vector()'

    
    logging.info("\n--- Test Case 2: Retrieving full Vector objects by ID ---")

    if inserted_vector_ids:

        ids_to_retrieve = inserted_vector_ids[:min(3, len(inserted_vector_ids))]
        logging.info(f"Attempting to retrieve {len(ids_to_retrieve)} vectors")
        for id in ids_to_retrieve:
            logging.info(f"Retrieving vector data for vector id : {id}")
            retrieved_vector = db_manager.get_full_vector_data_by_id(id)
            if retrieved_vector:
                logging.info(retrieved_vector)
                logging.info(f"Dictionary representation: {retrieved_vector.to_dict()}")
            time.sleep(0.05)

    else:
        logging.warning("No vectors were retrieved for that ID")
            
            
                                              

       



