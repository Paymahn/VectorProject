import sqlite3
import logging
from vector_models import Vector

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class DBManager : 
    def __init__(self, db_connection_information):
        self.db_connection_information = db_connection_information
        logging.debug(f"DBManager initialized with connection info: {self.db_connection_information}")

    def _get_connection(self):
        logging.debug("Establishing database connection.")
        return sqlite3.connect(self.db_connection_information, timeout=10)
    
    def execute_query(self, query, params=()):
        logging.debug(f"Executing query: {query}")
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(query, params)
                conn.commit()
                logging.info("Query executed successfully.")
                return cursor
        except sqlite3.Error as e:
            logging.error(f"An error occurred while executing the query: {e}")
            raise

# will call execute query to insert the vector data into two different tables in our database. 
    def insert_vector_data(self, vector_object):
        logging.debug(f"Inserting vector data: {vector_object}")
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()

                # Insert into vectors metadata table
                cursor.execute('''
                    INSERT INTO vectors_metadata (length, color, shape, creation_timestamp)
                    VALUES (?, ?, ?, CURRENT_TIMESTAMP)
                ''', (vector_object.length, vector_object.color, vector_object.shape))
                
                vector_id = cursor.lastrowid
                processed_dimensions = vector_object.dimensions.copy()
                if len(processed_dimensions) < 5:
                    processed_dimensions += [0] * (5 - len(processed_dimensions))  # Pad with zeros if less than 5 dimensions
                processed_dimensions = processed_dimensions[:5]  # Ensure we only take the first 5 digits


                logging.info(f"Inserted vector metadata with ID: {vector_id}")

                dimensions_to_insert = []
                
                # Insert into vector_dimensions table
                for index, value in enumerate(processed_dimensions):
                    dimensions_to_insert.append((vector_id, index, value))

                cursor.executemany('''
                        INSERT INTO vector_dimensions (vector_id, dimension_index, dimension_value)
                        VALUES (?, ?, ?)
                 ''', dimensions_to_insert)
                
                logging.info("Inserted vector dimensions successfully.")
                conn.commit()
                return vector_id
        except sqlite3.Error as e:
            logging.error(f"An error occurred while inserting vector data: {e}")
            return None
            

            # Low level method to fetch data from the database using a query and parameters.
    def fetch_query(self, query, params=()):
        logging.debug(f"Fetching data with query: {query}")
        try:
            with self._get_connection() as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                cursor.execute(query, params)
                results = cursor.fetchall()
                logging.info("Data fetched successfully.")
                return [dict(row) for row in results]  # Convert rows to dictionaries for easier access
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
        try: 
            with self._get_connection() as conn:
                cursor = conn.cursor()

                # Table for main vector metadata
                cursor.execute('''
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
                cursor.execute('''
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
                cursor.execute('''
                    CREATE INDEX IF NOT EXISTS idx_vector_dimensions_lookup
                    ON vector_dimensions (dimension_index, dimension_value)
                ''')
                logging.info("Index for vector dimensions created successfully.")

                cursor.execute('''
                               CREATE INDEX IF NOT EXISTS idx_vectors_color
                                 ON vectors_metadata (color)
                                 ''')
                logging.info("Index for vector colors created successfully.")
                cursor.execute('''
                               CREATE INDEX IF NOT EXISTS idx_vectors_shape
                                 ON vectors_metadata (shape)
                                 ''')
                logging.info("Index for vector shapes created successfully.")

        except sqlite3.Error as e:
            logging.error(f"An error occurred while creating tables: {e}")
            raise
        logging.debug("All tables created successfully.")





    
        



        