import dill
import codecs
import redis
from pydantic import BaseModel
from fastapi import FastAPI, HTTPException
from contextlib import asynccontextmanager
from uuid import uuid4 
import uuid
import json

''' Fast API:
- Connect to Redis
- Define request/response models
- Implement endpoints
- Publish new tasks to Redis pub/sub
'''


'''
Client Action (HTTP)	Service Endpoint	        Service Action (Redis)
Register function	    /register_function (POST)	Service calls r.set()
Execute function	    /execute_function (POST)	Service calls r.set() and r.publish()
Get result	            /result/<task_id> (GET)	    Service calls r.get()
'''

r: redis.Redis = None 
app = FastAPI()


# dill pickling
def serialize(obj) -> str:
    '''convert object to string w dill'''
    return codecs.encode(dill.dumps(obj), "base64").decode()

def deserialize(s: str):
    '''convert base64 string back to object'''
    return dill.loads(codecs.decode(s.encode(), "base64"))

# Pydantic model for item data
# register request
class RequestFunction(BaseModel):
    id: str
    name: str
    payload: str
    functionMethod: str
    params: list

# register response
class ResponseFunction(BaseModel):
    function_id: uuid.UUID


@asynccontextmanager
async def lifespan(app: FastAPI):
    '''FastAPI lifespan handler: run at app start (before yield) and shutdown (after yield) '''
    global r
    
    REDIS_HOST = "localhost" 
    REDIS_PORT = 6379

    try:
        # initialize redis connection object
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        # test if redis is available
        r.ping()
        print(f"Successfully connected to Redis at {REDIS_HOST}:{REDIS_PORT}")
    except redis.exceptions.ConnectionError:
        print(f"Could not connect to Redis at {REDIS_HOST}:{REDIS_PORT}")
        r = None 

    # app is ready to take requests
    yield

    # shutdown after yield
    print("FastAPI application shutting down.")
    
app = FastAPI(lifespan=lifespan)

@app.get("/")
async def root():
    '''Check if API is running and redis connection status '''
    redis_status = "Disconnected"
    try:
        if r and r.ping():
            redis_status = "Connected and ping works"
    except Exception:
        # If ping fails
        redis_status = "Connection Failed"

    return {
        "message": "FaaS Service is running", 
        "redis_status": redis_status
    }


# regsiter new function by storing serialized payload in redis
@app.post("/register_function/")
async def register_function(function: RequestFunction):

    # key for storage, unique id for function
    function_id = str(uuid4())

    # to be stored in redis
    function_stored = {
        "id": function.id,
        "name": function.name,
        "payload": function.payload,
        "functionMethod": function.functionMethod,
        "params": function.params
    }

    redis_func_key = function_id

    # store in Redis
    try:
        r.set(redis_func_key, json.dumps(function_stored))
        print(f"Registered {function_stored['name']} with ID {function_id}")        
        return {"function_id": function_id}
    except Exception as e:
        print(f"Redis function registration error: {e}")


# In-memory database (for demonstration purposes)
functions = []

@app.post("/function/")
async def run_function(function: RequestFunction):
    functionCode = deserialize(function.functionMethod)
    # functions.append(functionCode)
    # result = functionCode()
    print(functions)
    print(function.name)
    print(function.id)
    print(function.params)
    result = functionCode(*function.params)
    print(result)
    return result

# test redis get
@app.get("/redis-test/{key}")
async def redis_test(key: str):
    if not r:
        raise HTTPException(status_code=503, detail="Redis service unavailable")
    
    value = r.get(key)
    
    if value is None:
        return {"key": key, "message": "Key not found in Redis."} 
    
    return {"key": key, "value": value}
