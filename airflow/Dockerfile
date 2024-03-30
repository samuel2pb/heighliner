FROM apache/airflow:2.8.3

# Create a directory for your project
WORKDIR /opt/airflow

# Create .env file
RUN echo -e "AIRFLOW_UID=$(id -u)" > .env

# Copy the requirements file and wheel file into the image
COPY airflow/requirements.txt ./
COPY ./dist/heighliner-0.1.0-py3-none-any.whl .

# Assuming yaml directory is within the build context, add it to the desired location
ADD airflow/yaml ./yaml
ENV AIRFLOW_YAML_FILES_PATH=/opt/airflow/yaml

# Install dependencies from requirements.txt and the wheel file
RUN pip install -r requirements.txt
RUN pip install heighliner-0.1.0-py3-none-any.whl
