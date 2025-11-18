from fastapi import FastAPI
from pydantic import BaseModel
import json

app = FastAPI()


@app.get("/")
async def read_root():
    return {"Hello": "World!"}


# In-memory database (for demonstration purposes)
functions = []


# Pydantic model for item data
class Function(BaseModel):
    id: str
    name: str
    functionBody: str


@app.post("/function/", response_model=Function)
async def create_item(function: Function):
    functions.append(function)
    return {"message": "Function registered successfully!"}
