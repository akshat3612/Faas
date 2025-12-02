import dill
import codecs
import redis
from pydantic import BaseModel
from fastapi import FastAPI, HTTPException
from contextlib import asynccontextmanager
from uuid import uuid4 
import uuid
import json

r: redis.Redis = None 
app = FastAPI()

# dill pickling
def serialize(obj) -> str:
    '''convert object to string w dill'''
    return codecs.encode(dill.dumps(obj), "base64").decode().strip()

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

    # store serialized function payload
    function_stored = {
        "payload": function.payload #dill func
    }

    try:
        r.set(function_id, json.dumps(function_stored))
        print(f"Registered function with ID {function_id}")
        return {"function_id": function_id}

    except Exception as e:
        print("Redis function registration error:", e)
        raise HTTPException(status_code=500, detail="Function registration failed")

# execute a registered function through dispatcher/workers
@app.post("/execute_function/")
async def execute_function(req: ExecuteFnReq):

    task_id = str(uuid4())

    # build unified task object
    task_stored = {
        "task_id": task_id,
        "function_id": req.function_id,
        "payload": req.payload,   # serialized args
        "status": "QUEUED"
    }

    r.set(f"task:{task_id}", json.dumps(task_stored))

    r.publish("tasks", task_id)

    return {"task_id": task_id}


# get result of executed task
@app.get("/result/{task_id}")
async def get_result(task_id: str):
    task_key = f"task:{task_id}"
    task_raw = r.get(task_key)
    
    if task_raw is None:
        raise HTTPException(status_code=404, detail="Task not found")
    
    task = json.loads(task_raw)
    status = task.get("status")
    
    # if task not done, report status
    if status != "COMPLETE":
        return {"task_id": task_id, "status": status}
    
    result_raw = task.get("result")
    
    if result_raw is None:
        return {
            "task_id": task_id,
            "status": "RUNNING"
        }
    
    # clean and deserialize result
    cleaned = result_raw.strip().replace("\n", "")
    try:
        result = deserialize(cleaned)
    except Exception as e:
        return {
            "task_id": task_id,
            "status": "FAILED",
            "exception": f"Deserialization error: {e}"
        }
    
    return {
        "task_id": task_id,
        "status": "COMPLETE",
        "result": result
    }

# test redis get
@app.get("/redis-test/{key}")
async def redis_test(key: str):
    if not r:
        raise HTTPException(status_code=503, detail="Redis service unavailable")
    
    value = r.get(key)
    
    if value is None:
        return {"key": key, "message": "Key not found in Redis."} 
    
    return {"key": key, "value": value}