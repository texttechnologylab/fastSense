FROM python:3.6

WORKDIR /usr/src/app

RUN apt-get update && apt-get install -y --no-install-recommends build-essential openjdk-8-jdk openjdk-8-jre-headless && rm -rf /var/lib/apt/lists/*
RUN pip install cython

COPY disambig_server/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY disambig_server/ ./disambig_server
COPY ned/ ./ned

COPY setup.py ./
RUN python setup.py install

EXPOSE 80

VOLUME [ "/model" ]

ENTRYPOINT [ "python", "disambig_server/disambig_server.py", "--model", "/model/", "--worker_count" ]
CMD [ "1" ]
