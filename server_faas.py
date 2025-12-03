import dill
import codecs
import redis
from pydantic import BaseModel
from fastapi import FastAPI, HTTPException, status
from contextlib import asynccontextmanager
from uuid import uuid4
import uuid
import json

r: redis.Redis = None
app = FastAPI()


# dill pickling
def serialize(obj) -> str:
    return codecs.encode(dill.dumps(obj), "base64").decode().strip()


def deserialize(s: str):
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
# FastAPI lifespan handler: run at app start and shutdown
async def lifespan(app: FastAPI):

    global r

    REDIS_HOST = "localhost"
    REDIS_PORT = 6379

    try:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        r.ping()
        print(f"Successfully connected to Redis at {REDIS_HOST}:{REDIS_PORT}")
    except redis.exceptions.ConnectionError as e:
        print(f"Could not connect to Redis at {REDIS_HOST}:{REDIS_PORT}")
        print(f"Error: {e}")
        r = None

    yield

    print("FastAPI application shutting down.")


app = FastAPI(lifespan=lifespan)


@app.get("/")
# check if API is running and redis connection status
async def root():
    redis_status = "Disconnected"
    try:
        if r and r.ping():
            redis_status = "Connected and ping works"
    except Exception:
        redis_status = "Connection Failed"

    return {"message": "FaaS Service is running", "redis_status": redis_status}


# register new function by storing serialized payload in redis
@app.post("/register_function/")
async def register_function(function: RequestFunction):

    # redis connection
    if r is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Redis service is unavailable. Cannot register function.",
        )

    # payload not empty
    if not function.payload or function.payload.strip() == "":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Function payload cannot be empty",
        )

    # payload is valid base64 serialized function
    try:
        deserialize(function.payload)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid function payload. Must be a valid base64-encoded serialized function. Error: {str(e)}",
        )

    function_id = str(uuid4())

    # store serialized function payload
    function_stored = {"payload": function.payload}  # dill func

    try:
        r.set(function_id, json.dumps(function_stored))
        print(f"Registered function with ID {function_id}")
        return {"function_id": function_id}
    except redis.exceptions.RedisError as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to register function in Redis: {str(e)}",
        )

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Unexpected error during function registration: {str(e)}",
        )


# execute a registered function through dispatcher/workers
@app.post("/execute_function/", status_code=status.HTTP_202_ACCEPTED)
async def execute_function(req: ExecuteFnReq):
    # check redis connection
    if r is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Redis service is unavailable. Cannot execute function.",
        )

    # validate format
    try:
        uuid.UUID(req.function_id)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid function_id format: '{req.function_id}'. Must be a valid UUID.",
        )

    # check if function exists
    try:
        function_data = r.get(req.function_id)
        if function_data is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Function with ID '{req.function_id}' not found. Please register the function first.",
            )
    except redis.exceptions.RedisError as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error checking function existence in Redis: {str(e)}",
        )

    # validate payload is not empty
    if not req.payload or req.payload.strip() == "":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Function arguments payload cannot be empty",
        )

    # payload is valid base64 serialized arguments
    try:
        deserialize(req.payload)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid arguments payload. Must be valid base64-encoded serialized arguments. Error: {str(e)}",
        )

    task_id = str(uuid4())

    task_stored = {
        "task_id": task_id,
        "function_id": req.function_id,
        "payload": req.payload,  # serialized args
        "status": "QUEUED",
    }

    try:
        r.set(f"task:{task_id}", json.dumps(task_stored))

        # oublish after storing
        r.publish("tasks", task_id)

        return {"task_id": task_id}
    except redis.exceptions.RedisError as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to queue task in Redis: {str(e)}",
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Unexpected error during task execution: {str(e)}",
        )


# get result of executed task
@app.get("/result/{task_id}")
async def get_result(task_id: str):
    # check redis connection
    if r is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Redis service is unavailable. Cannot retrieve task result.",
        )

    # validate task_id
    try:
        uuid.UUID(task_id)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid task_id format: '{task_id}'. Must be a valid UUID.",
        )

    task_key = f"task:{task_id}"

    try:
        task_raw = r.get(task_key)
    except redis.exceptions.RedisError as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error retrieving task from Redis: {str(e)}",
        )

    if task_raw is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Task with ID '{task_id}' not found.",
        )

    try:
        task = json.loads(task_raw)
    except json.JSONDecodeError as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Cannot parse JSON: {str(e)}",
        )

    status_val = task.get("status")

    if status_val is None:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Task data is missing 'status' field",
        )

    # just report status if task not done
    if status_val not in ["COMPLETE", "COMPLETED"]:
        return {"task_id": task_id, "status": status_val}

    result_raw = task.get("result")

    if result_raw is None:
        # complete but no result available
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Task marked as COMPLETE but result data is missing",
        )

    cleaned = result_raw.strip().replace("\n", "")
    try:
        result = deserialize(cleaned)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to deserialize task result: {str(e)}",
        )

    return {"task_id": task_id, "status": status_val, "result": result}


# test redis get
@app.get("/redis-test/{key}")
async def redis_test(key: str):
    if not r:
        raise HTTPException(status_code=503, detail="Redis service unavailable")

    value = r.get(key)

    if value is None:
        return {"key": key, "message": "Key not found in Redis."}

    return {"key": key, "value": value}
