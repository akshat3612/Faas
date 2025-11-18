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
    functionCode: str


@app.post("/function/")
async def run_function(function: Function):
    print(function)
    functions.append(function)
    print(function.functionCode)
    return exec(function.functionCode)
