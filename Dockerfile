FROM apache/airflow:2.5.0

USER root

# runtime dependencies
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
        vim \
        build-essential \
        libopenmpi-dev \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

USER airflow

COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt

# COPY --chown=airflow:root /dags/test_dag.py /opt/airflow/dags

# COPY . .

# EXPOSE 3000

# CMD [ "python", "./your-daemon-or-script.py" ]