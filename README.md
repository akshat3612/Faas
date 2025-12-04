# MPCS FaaS Project Starter Code

This is starter code for the MPCS52040 FaaS project for Fall 2025.
This repo supplies:

1. A `pyproject.toml` file that gets you started with installing a python package
2. `mpcsfaas.serialize` package that contains serialization code
3. Basic starter tests


## Installing the project

Please ensure that your code is installable with the following command:

```
pip install .
```

## Recommendations

1. Consider using `ruff` or a similar linter
2. Type hints are your friend
3. Consider modularizing code with splitting code into modules## FaaS Project - Distributed Systems

- Authored by Akshat Singhania & Linn Oberbeck

-----
Prerequisites
```
pip install fastapi uvicorn dill
brew install redis
```
----

### Design Overview

1) Simple FastAPI Server

```
uvicorn server:app --reload
```


### Test Instructions
1) Start the server Application
2) Run the sample_code.py to generate the serialized string of function
3) Use this along with other parameters in Postman to test the Serverless Code Execution
4) Successful results end with the output [200 JSON Output]



### TO-DO

- Complete Client-Side [DONE]
- Implement Redis Caching [with UUID] [DONE]
- Implement Task Dispatcher [DONE]
- Implement Task Lifecycle [DONE]
- Implement Push-Pull Workers using ZMQ [DONE]
- Brainstorm Fault Tolerance
- Performance Evaluation
- Creating Reports 
- Running Tests

### This was the very beginning, add to report at most
# To run the Redis server

```
redis-server
```

```
python3 -m uvicorn redis_faas:app --reload
```

In browser:
```
http://127.0.0.1:8000/
```

# To run client

```
python3 client_faas.py
```

I tested the connection first by:
```
http://127.0.0.1:8000/redis-test/testkey
```

Then by:
```
redis-cli set user:1 "Linn"
```

And the HTTP http://127.0.0.1:8000/redis-test/user:1 shows:
```
{
  "key": "user:1",
  "value": "Linn"
}
```
### How to actually run everything now:

Have 5 terminals open.

In the first one to start Redis, run
```
redis-server
```

In the second window, start the dispatcher with either push or pull tasks:
```
 python3 task_dispatcher.py -m pull
 python3 task_dispatcher.py -m push
```

In the next window, start the server:

```
uvicorn server_faas:app --host 127.0.0.1 --port 8000
```

Then, in a new terminal, run either the push or pull workers:
```
python3 pull_workers.py
python3 push_workers.py
```

And finally, start the client with a task:
```
python3 client_faas.py "lambda x: x*x" 3
```

### additional testing


- client sends task, gets registered by dispatcher, but not registered in redis:
```
PullDispatcher subscribed to Redis channel tasks
New task queued: 52b7f909-7215-4598-a255-d755c7aaaf3a
Task 52b7f909-7215-4598-a255-d755c7aaaf3a not found in Redis
New task queued: bbe47bd9-7d04-4673-9300-a49bbaf0f342
Task bbe47bd9-7d04-4673-9300-a49bbaf0f342 not found in Redis
```
Solved by aligning fields to be saved that were differently stored in dispatcher and server, push and pull workers

- manually look into redis to investigate why tasks arent completed successfully:
```
redis-cli
127.0.0.1:6379> keys *278b5f3e*
1) "task:278b5f3e-99cc-40bb-8820-a38bc59eafb2"
127.0.0.1:6379> get task:278b5f3e-99cc-40bb-8820-a38bc59eafb2
"{\"task_id\": \"278b5f3e-99cc-40bb-8820-a38bc59eafb2\", \"function_id\": \"8001622b-730d-4f50-87ca-7f044669450b\", \"payload\": \"gASVBwAAAAAAAABLCksUhpQu\\n\", \"status\": \"FAILED\", \"result\": \"gARLHi4=\\n\"}"
127.0.0.1:6379> 
```

- heartbeats between workers and dispatcher had to be aligned

- kill push worker:Worker push-1764714319 missed heartbeat, marking as dead
start again:Worker push-1764714387 registered
