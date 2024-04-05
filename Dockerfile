FROM docker.io/library/python:3.11-slim

ADD requirements.txt /code/requirements.txt
RUN pip install -r /code/requirements.txt

ADD download_artifacts.py /code/download_artifacts.py

ENTRYPOINT ["python", "/code/download_artifacts.py"]
