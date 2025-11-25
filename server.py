from fastapi import FastAPI
from pydantic import BaseModel
import dill
import codecs

app = FastAPI()


# To be used by the the client ot Serialize the Function to Send
def serialize(obj) -> str:
    return codecs.encode(dill.dumps(obj), "base64").decode()


# used by the server to de-serialize the function received and execute
def deserialize(obj: str):
    return dill.loads(codecs.decode(obj.encode(), "base64"))


@app.get("/")
async def read_root():
    return {"Hello": "World!"}


# In-memory database (for demonstration purposes)
functions = []


# Pydantic model for item data
class Function(BaseModel):
    id: str
    name: str
    functionMethod: str
    description: str
    params: list


@app.post("/function/")
async def run_function(function: Function):
    functionCode = deserialize(function.functionMethod)
    # functions.append(functionCode)
    # result = functionCode()
    print(functions)
    print(function.name)
    print(function.id)
    print(function.description)
    print(function.params)
    result = functionCode(*function.params)
    print(result)
    return result
