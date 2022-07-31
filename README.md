# Ensembl's data lakehouse backend | GSoC '22

Recommended: Python 3.9.13

Run the script via  
- `Command line`:
1. Optional: Setup a python virtual environment
2. Install all the required packages: `pip3 install -r requirements.txt`
3. Start a Redis server on localhost: `docker run -p 6379:6379 -it redis/redis-stack-server:latest`
   OR set custom env. variables values for `REDIS_HOST` and `REDIS_PORT`
4. Setup your AWS keys as explained here: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/quickstart.html#configuration (config. location path: `~/.aws/` [`~` -> Root directory])
5. Start a celery worker:  
   a. Linux / Mac: `celery -A app.tasks worker --loglevel=DEBUG --logfile=log_celery.txt --concurrency=2`  
   b. Windows: `celery -A app.tasks worker --loglevel=DEBUG --logfile=log_celery.txt --concurrency=2 --pool=solo` (Extra flag is needed as a workaround as celery doesn't support Windows anymore)  
   c. Available command-line options: `celery worker --help`
6. Start the app via `uvicorn app.main:app --reload`
- `Dockerfile`:
1. Update your AWS keys in `.aws/credentials` [`.aws` directory should be in same directory as the `Dockerfile`]
2. Build the image from the dockerfile via `docker build --tag e-lakehouse .`
3. Run the container via `docker run -d --name e-lakehouse -p 80:80 -e REDIS_HOST="<custom_redis_host>" -e REDIS_PORT=<custom_redis_port> e-lakehouse`
4. Setup a celery worker on same / different machine

Dependency: `Redis`  
Default value for env. vars.:  
`REDIS_HOST` = `localhost`  
`REDIS_PORT` = `6379`  

---

`.aws` configuration files content for reference:

`config`  
[default]  
region=eu-west-2

`credentials`  
[default]  
aws_access_key_id = YOUR_ACCESS_KEY  
aws_secret_access_key = YOUR_SECRET_KEY

---

OpenAPI doc: {base_url}/docs