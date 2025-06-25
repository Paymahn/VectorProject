import random
import sqlite3
import logging
import datetime

colors = ["red", "green", "blue"]
shapes = ["circle", "square", "triangle"]


#Vector class creation (Functionality includes conversion from database optimised storage to Vector object, and vector object to python dictionary for front end rendering)

class Vector :
    def __init__(self, dimensions, color, shape, timestamp=None):
        if not isinstance(dimensions, list) or not all(isinstance(individual_dimension, int) for individual_dimension in dimensions):
            raise ValueError("Dimensions must be a list of integers.")
        self.id = None
        self.dimensions = dimensions
        self.color = color
        self.shape = shape
        self.length = len(dimensions)
        self.creation_timestamp = timestamp if timestamp else datetime.datetime.now(datetime.timezone.utc)

    def __repr__(self):
        return f"Vector(dimensions={self.dimensions}, color='{self.color}', shape='{self.shape}', length={self.length}, creation_timestamp='{self.creation_timestamp}')"  

    # Method that converts vector to dictionary format for front end display and JSON serialization
    def to_dict(self) -> dict:
        return {
            'id' : self.id,
            'dimensions': self.dimensions,
            'color': self.color,
            'shape': self.shape,
            'length': self.length,
            'creation_timestamp': self.creation_timestamp.isoformat()
        
        }
    
    # Method that reconstructs a vector object from the database record.
    @classmethod
    def load_from_db_record_into_object(cls, vector_metadata_raw : dict, vector_dimensions_raw : list[dict]):
        logging.debug(f"Loading vector from DB record: {vector_metadata_raw} + {vector_dimensions_raw}")

         # Extract basic metadata
        vector_id = vector_metadata_raw['id']
        color = vector_metadata_raw['color']
        shape = vector_metadata_raw['shape']

        # Extract and convert creation_timestamp
        creation_timestamp_str = vector_metadata_raw['creation_timestamp']
        creation_timestamp_obj = datetime.datetime.fromisoformat(creation_timestamp_str)

        # Extract vector dimensions and put in Vector format (list)
        dimensions_list_of_ints = [dim['dimension_value'] for dim in vector_dimensions_raw]
        
        reconstructed_vector = cls(
            color = color,
            shape = shape,
            timestamp = creation_timestamp_obj,
            dimensions = dimensions_list_of_ints,
        )

        reconstructed_vector.id = vector_id
        return reconstructed_vector




# Create one instance of a vector

def generate_one_vector() :
    total_dimensions = random.randint(2,5)
    dimensions_array = [random.randint(1, 10) for _ in range(total_dimensions)]
    vector_color = random.choice(colors)
    vector_shape = random.choice(shapes)
    vector = Vector(dimensions_array, vector_color, vector_shape)
    return vector













