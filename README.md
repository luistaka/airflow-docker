# airflow-docker

# Python Version
python 3.9.16

# Create VENV 
python3 -m venv /Users/luis.takahashi/Desktop/Tools/Git/airflow-docker

# Activate VENV
source /Users/luis.takahashi/Desktop/Tools/Git/airflow-docker/bin/activate

# Create requirement
pip freeze > requirements.txt

# Install requirements
pip install -r requirements.txt
pip install --upgrade --force-reinstall -r requirements.txt

# See list of libraries installed
pip list

# Deactivate VENV
deactivate

# Building Docker images
docker build -t my-airflow-image .
docker build . -f Dockerfile --pull --tag projeto-aplicado-airflow-image:0.0.1

# Test Image
<!-- ./scripts/ci/tools/verify_docker_image.sh PROD my-airflow-image:0.0.1 -->

# Running a Docker image by using the docker run API
docker run -p80:3000 --name Projeto-Aplicado-Airflow projeto-aplicado-airflow-image
docker run -p80:3000 -it --rm --name Projeto-Aplicado-Airflow projeto-aplicado-airflow-image

# Running a Docker image in a detached mode
docker run -d -p8080:8080 --name Projeto-Aplicado-Airflow projeto-aplicado-airflow-image

# Running a Docker image in the container indefinitely
docker run -d -t -p8080:8080 --name Projeto-Aplicado-Airflow projeto-aplicado-airflow-image

# Running a Docker image in bash
docker run -p8080:8080 -it --name Projeto-Aplicado-Airflow projeto-aplicado-airflow-image

# Create docker-compose.yaml
<!-- (https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html) -->
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.5.0/docker-compose.yaml'


# Create docker-context-files
curl -Lo "docker-context-files/constraints-3.7.txt" \
    "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-3.7.txt"

# Initialize the database
docker compose up airflow-init

# Running Airflow
docker-compose up

# Enter the Airflow container
docker exec -it <container-id> bash

# This command will stop and delete all running containers, delete volumes with database data and downloaded images
docker-compose down --volumes --rmi all

# Remove all exited containers
docker rm $(docker ps -a -q -f status=exited)