FROM python:3.9

COPY requirements.txt requirements.txt

RUN pip3 install -r requirements.txt

COPY .aws /root/.aws
COPY main.py main.py

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "80"]
