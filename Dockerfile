FROM prefecthq/prefect

ADD requirements.txt /requirements.txt

RUN pip install -r /requirements.txt