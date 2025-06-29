import os
import logging
from fastapi import FastAPI, HTTPException 
from typing import Optional, List, Dict 
from pydantic import BaseModel 
from contextlib import asynccontextmanager 
from .vector_models import Vector
from .database_manager import DBManager 


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
 
DB_FILE_NAME = "my_vector_database.db"
db_manager : DBManager = None


@asynccontextmanager
async def lifespan(app : FastAPI):
    global db_manager
    db_manager = DBManager(DB_FILE_NAME)
    db_manager.connect_to_database()
    logging.info("Initialised DB manager in FastAPI")
    db_manager.create_tables()
    logging.info("Database tables created")
    yield
    db_manager.close_connection()
    logging.info("Closing DB manager in FastAPI")


app = FastAPI(lifespan=lifespan)

class VectorCreate(BaseModel):
    dimensions : list[int] 
    color : str
    shape : str

class VectorResponse(BaseModel):
    id: int
    dimensions: List[int]
    color: str
    shape: str
    length: int
    creation_timestamp: str 

@app.get("/")
async def rootpage():
    return {"message": "Vector API is running!"}


#Will use this as an endpoint to create a "vector" on the Admin portion of the vector UI
@app.post(path = "/vectors/", response_model=VectorResponse, status_code=201)
async def create_vector(vector_data : VectorCreate):
    try:
        new_vector_object = Vector(dimensions=vector_data.dimensions, color = vector_data.color, shape = vector_data.shape)
        new_vector_id = db_manager.insert_vector_data(new_vector_object)
        
        if new_vector_id is None:
            logging.error(f"Database insertion failed: insert_vector_data returned None for vector data.")
            raise HTTPException(status_code=500, detail=f"Internal server error")

        new_vector_object.id = new_vector_id
        return new_vector_object.to_dict()

    except Exception as e:        
        logging.error("Unexpected vector creation error : {e}", exc_info=True)
        raise HTTPException(status_code=500, detail ="An unexpected internal server error occurred.")
