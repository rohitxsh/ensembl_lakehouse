# Ensembl's data lakehouse backend | GSoC '22

Recommended: Python 3.x.x

Run the script via  
- `Command line`:
1. Start a Redis server on localhost: `docker run -p 6379:6379 -it redis/redis-stack-server:latest`
   OR set custom env. variables values for `REDIS_HOST` and `REDIS_PORT`
2. Setup your AWS keys as explained here: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/quickstart.html#configuration (config. location path: `~/.aws/` [`~` -> Root directory])
3. Run the script via `uvicorn app.main:app --reload`
- `Dockerfile`:
1. Update your AWS keys in `.aws/credentials` [`.aws` directory should be in same directory as the `Dockerfile`]
2. Build the image from the dockerfile via `docker build --tag e-lakehouse .`
3. Run the container via `docker run -d --name e-lakehouse -p 80:80 -e REDIS_HOST="<custom_redis_host>" -e REDIS_PORT=<custom_redis_port> e-lakehouse`

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