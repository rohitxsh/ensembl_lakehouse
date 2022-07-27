FROM python:3.9

COPY requirements.txt requirements.txt
COPY .aws /root/.aws
COPY app /app

RUN pip3 install -r requirements.txt

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "80"]
