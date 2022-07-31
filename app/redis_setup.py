from redis import Redis
import os

redis_host = os.getenv("REDIS_HOST", "localhost")
redis_port = os.getenv("REDIS_PORT", 6379)
r = Redis(host=redis_host, port=redis_port, db=0)