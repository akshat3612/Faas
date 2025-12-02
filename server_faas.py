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

# register request
class RequestFunction(BaseModel):
    id: str
    name: str
    payload: str
    functionMethod: str
    params: list

class ExecuteFnReq(BaseModel):
    function_id: str
    payload: str

# register response
class ResponseFunction(BaseModel):
    function_id: uuid.UUID

# execute request
class ExecuteRequest(BaseModel):
    function_id: str
    params: list


@asynccontextmanager
async def lifespan(app: FastAPI):
    '''FastAPI lifespan handler: run at app start (before yield) and shutdown (after yield) '''
    global r
    
    REDIS_HOST = "localhost" 
    REDIS_PORT = 6379

    try:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        r.ping()
        print(f"Successfully connected to Redis at {REDIS_HOST}:{REDIS_PORT}")
    except redis.exceptions.ConnectionError:
        print(f"Could not connect to Redis at {REDIS_HOST}:{REDIS_PORT}")
        r = None 

    yield

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
        redis_status = "Connection Failed"

    return {
        "message": "FaaS Service is running", 
        "redis_status": redis_status
    }

# register new function by storing serialized payload in redis
@app.post("/register_function/")
async def register_function(function: RequestFunction):

    function_id = str(uuid4())

    function_stored = {
        "id": function.id,
        "name": function.name,
        "payload": function.payload,
        "functionMethod": function.functionMethod,
        "params": function.params
    }

    redis_func_key = function_id

    try:
        r.set(redis_func_key, json.dumps(function_stored))
        print(f"Registered {function_stored['name']} with ID {function_id}")        
        return {"function_id": function_id}
    except Exception as e:
        print(f"Redis function registration error: {e}")

# execute a registered function through dispatcher/workers
@app.post("/execute_function/")
async def execute_function(req: ExecuteFnReq):

    task_id = str(uuid4())

    status_key = f"task:{task_id}:status"
    fn_key = f"task:{task_id}:fn"
    param_key = f"task:{task_id}:params"

    # store function payload and argument payload in Redis
    r.set(status_key, "QUEUED")
    r.set(fn_key, r.get(req.function_id))   # registered fn metadata
    r.set(param_key, req.payload)   # serialized params

    # publish task id to dispatcher
    r.publish("tasks", task_id)

    return {"task_id": task_id}

# get result of executed task
@app.get("/result/{task_id}")
async def get_result(task_id: str):

    status_key = f"task:{task_id}:status"
    result_key = f"task:{task_id}:result"

    status = r.get(status_key)
    if status is None:
        raise HTTPException(status_code=404, detail="Task not found")

    if status != "COMPLETE":
        return {"task_id": task_id, "status": status}

    raw = r.get(result_key)
    result = deserialize(raw)

    return {"task_id": task_id, "status": "COMPLETE", "result": result}

# test redis get
@app.get("/redis-test/{key}")
async def redis_test(key: str):
    if not r:
        raise HTTPException(status_code=503, detail="Redis service unavailable")
    
    value = r.get(key)
    
    if value is None:
        return {"key": key, "message": "Key not found in Redis."} 
    
    return {"key": key, "value": value}