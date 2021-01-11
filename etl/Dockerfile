# set base image (host OS)
FROM python:3.8

# set the working directory in the container
WORKDIR /etl
RUN mkdir /etl/src
RUN mkdir /etl/data

# copy the dependencies file to the working directory
COPY requirements.txt .

# install dependencies
RUN pip install -r requirements.txt

# copy the content of the local src directory to the working directory
COPY run_all_etl.sh .
COPY src/ ./src/
COPY data/ ./data/

# command to run on container start
CMD [ "bash", "./run_all_etl.sh" ]