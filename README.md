## FaaS Project - Distributed Systems

- Authored by Akshat Singhania & Linn Oberbeck

-----
Prerequisites
```
pip install fastapi uvicorn dill
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

- Complete Client-Side
- Implement Redis Caching [with UUID]
- Implement Task Dispatcher
- Implement Task Lifecycle
- Implement Push-Pull Workers using ZMQ
- Brainstorm Fault Tolerance
- Performance Evaluation
- Creating Reports 
- Running Tests