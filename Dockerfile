FROM docker.io/library/python:3.11-slim

ADD . /code

RUN pip install -r /code/requirements.txt

CMD ["python", "/code/download_artifacts.py"]
