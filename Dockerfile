#Grab the latest Python3.6 image
FROM python:3.6.13-slim

# Install dependencies
RUN pip install --no-cache-dir -q sqllineage

# Run the image as a non-root user
RUN adduser --quiet sqllineage
USER sqllineage

# $PORT environment variable will be passed with --env in docker run command
CMD sqllineage -g -H 0.0.0.0 -p $PORT
