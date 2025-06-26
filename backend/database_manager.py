import sqlite3
import logging
import datetime
from .vector_models import Vector
from typing import Optional, List, Dict


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class DBManager : 
    def __init__(self, db_file_name : str):
        self.db_file_name = db_file_name
        self.conn : Optional[sqlite3.Connection] = None
        self.cursor : Optional[sqlite3.Cursor] = None
        logging.debug(f"DBManager instance created for {self.db_file_name}")

   
   # Method to create one continual connection with the database, that we use on launch of the fastAPI lifecycle
    def connect_to_database(self):
        try:
            self.conn = sqlite3.connect(self.db_file_name, timeout=10)
            self.conn.row_factory = sqlite3.Row
            self.cursor = self.conn.cursor()
            logging.info(f"Connected to database: {self.db_file_name}")
        except sqlite3.Error as e:
            logging.error(f"Database connection error: {e}")
            raise

    def close_connection(self):
        if self.conn:
            self.conn.close()
            self.conn = None
            self.cursor = None
            logging.info("Database connection closed")
        else:
            logging.warning("Attempt to close database unsuccessful, as no active connection found")
    
    def execute_query(self, query : str, params : tuple =()):
        if not self.conn or not self.cursor:
            logging.error("NO CURRENT ACTIVE DATABASE CONNECTION. CANNOT EXECUTE QUERY")
            raise sqlite3.Error("Ensure connect_to_database is called for active DB connection")
        
        logging.debug(f"Executing query: {query} with params : {params} ")
        try:
            self.cursor.execute(query, params)
            self.conn.commit()
            logging.info(f"Query : {query}, executed")
            return self.cursor
        except sqlite3.Error as e:
            logging.error("Error occured as query was executing")
            self.conn.rollback()
            raise





    
        
        


# will call execute query to insert the vector data into two different tables in our database. 
    def insert_vector_data(self, vector_object):
        if not self.conn or not self.cursor:
                logging.error("No active connection, cannot insert vector data")
                return None
        
        logging.debug(f"Inserting vector data: {vector_object}")
        
        try:
                # Insert into vectors metadata table
                self.cursor.execute('''
                    INSERT INTO vectors_metadata (length, color, shape, creation_timestamp)
                    VALUES (?, ?, ?, CURRENT_TIMESTAMP)
                ''', (vector_object.length, vector_object.color, vector_object.shape))
                
                vector_id = self.cursor.lastrowid
                processed_dimensions = vector_object.dimensions.copy()
                if len(processed_dimensions) < 5:
                    processed_dimensions += [0] * (5 - len(processed_dimensions))  
                processed_dimensions = processed_dimensions[:5]  


                logging.info(f"Inserted vector metadata with ID: {vector_id}")

                dimensions_to_insert = []
                
                # Insert into vector_dimensions table
                for index, value in enumerate(processed_dimensions):
                    dimensions_to_insert.append((vector_id, index, value))

                self.cursor.executemany('''
                        INSERT INTO vector_dimensions (vector_id, dimension_index, dimension_value)
                        VALUES (?, ?, ?)
                 ''', dimensions_to_insert)
                
                logging.info("Inserted vector dimensions successfully.")
                self.conn.commit()
                return vector_id
        except sqlite3.Error as e:
            logging.error(f"An error occurred while inserting vector data: {e}")
            self.conn.rollback()
            return None
            

            # Low level method to fetch data from the database using a query and parameters.
    def fetch_query(self, query, params=()):
        if not self.conn or not self.cursor:
            logging.error("No active connection, cannot fetch query")
            raise sqlite3.Error("No active database connection. call connect_to_database() .")
        
        logging.debug(f"Fetching data with query: {query} with params: {params}")

        try:
            
            self.cursor.execute(query, params) 
            results = self.cursor.fetchall() 
            logging.info("Data fetched successfully.")
            return [dict(row) for row in results] 
        except sqlite3.Error as e:
            logging.error(f"An error occurred while fetching data: {e}")
            raise 


       # Method to retrieve vector metadata by ID from vectors_metadata table.     
    def get_vector_metadata_by_id(self, vector_id):
        logging.debug(f"Fetching vector with ID: {vector_id}")
        query = '''
            SELECT * FROM vectors_metadata WHERE id = ?
        '''
        result = self.fetch_query(query, (vector_id,))
        if result:
            return result[0]
        else:
            logging.warning(f"No vector found with ID: {vector_id}")
            return None
    
    def get_vector_dimensions_by_id(self, vector_id):
        logging.debug(f"Fetching dimensions with vector ID: {vector_id}")
        query = '''
            SELECT * FROM vector_dimensions 
            WHERE vector_id = ?
            ORDER BY dimension_index ASC
        '''
        results = self.fetch_query(query, (vector_id,))
        if results:
            return results
        else:
            logging.warning(f"No dimensions found for vector ID: {vector_id}")
            return None
        

    # Method that retrieves full vector data, including metadata and dimensions, by vector ID.    
    def get_full_vector_data_by_id(self, vector_id):
        logging.debug(f"Fetching full vector data for ID: {vector_id}")
        metadata = self.get_vector_metadata_by_id(vector_id)
        dimensions = self.get_vector_dimensions_by_id(vector_id)

        if metadata is None or dimensions is None:
            logging.warning(f"Incomplete data for vector id: {vector_id}, metadata : {metadata is not None}")

        return Vector.load_from_db_record_into_object(metadata, dimensions)


    



    def create_tables(self):
        logging.debug("Attempting to create tables in the database.")
        if not self.conn or not self.cursor:
                logging.error("No active database connection, cannot create tables")
                raise sqlite3.Error("Cannot create tables, connect_to_database() must be called to ensure connection is active")
        
        try:
             # Table for main vector metadata
                self.cursor.execute('''
                    CREATE TABLE IF NOT EXISTS vectors_metadata (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        length INTEGER NOT NULL,
                        color TEXT NOT NULL,
                        shape TEXT NOT NULL,
                        creation_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                
                logging.info("Table for vectors metadata created successfully.")

                # Table for individual vector dimensions
                self.cursor.execute('''
                    CREATE TABLE IF NOT EXISTS vector_dimensions (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        vector_id INTEGER NOT NULL,
                        dimension_index INTEGER NOT NULL,
                        dimension_value INTEGER NOT NULL,
                        FOREIGN KEY (vector_id) REFERENCES vectors_metadata(id) ON DELETE CASCADE
                    )
                ''')
                logging.info("Table for vector dimensions created successfully.")

                # Create an index to speed up searches on specific dimensions
                self.cursor.execute('''
                    CREATE INDEX IF NOT EXISTS idx_vector_dimensions_lookup
                    ON vector_dimensions (dimension_index, dimension_value)
                ''')
                logging.info("Index for vector dimensions created successfully.")

                self.cursor.execute('''
                               CREATE INDEX IF NOT EXISTS idx_vectors_color
                                 ON vectors_metadata (color)
                                 ''')
                logging.info("Index for vector colors created successfully.")
                self.cursor.execute('''
                               CREATE INDEX IF NOT EXISTS idx_vectors_shape
                                 ON vectors_metadata (shape)
                                 ''')
                logging.info("Index for vector shapes created successfully.")

        except sqlite3.Error as e:
            logging.error(f"An error occurred while creating tables: {e}")
            self.conn.rollback()
            raise
        logging.debug("All tables created successfully.")





    
        



        