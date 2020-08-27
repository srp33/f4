FROM python:slim-buster

RUN pip install fastnumbers==3.0.0 joblib==0.16.0 zstandard==0.14.0
