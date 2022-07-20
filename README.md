# Ensembl's data lakehouse backend | GSoC '22

Recommended: Python 3.x.x

Run the script via
- FastAPI: `uvicorn main:app --reload`

Dependency:
- Redis: `docker run -p 6379:6379 -it redis/redis-stack-server:latest`


Boto3 requires valid AWS credentials  
Setup your AWS keys as explained here: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/quickstart.html#configuration (config. location path: ~/.aws/ [~ -> Root directory])
