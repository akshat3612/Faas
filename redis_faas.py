import dill
import codecs
import redis
from fastapi import FastAPI, HTTPException
from contextlib import asynccontextmanager
from uuid import uuid4

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


# test redis get
@app.get("/redis-test/{key}")
async def redis_test(key: str):
    if not r:
        raise HTTPException(status_code=503, detail="Redis service unavailable")
    
    value = r.get(key)
    
    if value is None:
        return {"key": key, "message": "Key not found in Redis."} 
    
    return {"key": key, "value": value}
