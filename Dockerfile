FROM nikolaik/python-nodejs:python3.10-nodejs18-slim

# copy source files to docker image
ARG CWD=/mnt/sqllineage
ADD sqllineage/ ${CWD}/sqllineage
ADD sqllineagejs/ ${CWD}/sqllineagejs
COPY pyproject.toml setup.py README.md ${CWD}/
WORKDIR ${CWD}

# install and remove all source code
RUN pip install ${CWD}  \
    && rm -rf ${CWD}/*

# Run the image as a non-root user
RUN adduser --quiet sqllineage
USER sqllineage

# $PORT environment variable will be passed with --env in docker run command
CMD sqllineage -g -H 0.0.0.0 -p $PORT
