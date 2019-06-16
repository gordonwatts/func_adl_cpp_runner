# Build a docker image to run against ATLAS code that has been pre-built and is ready to go.

FROM atlas/analysisbase:21.2.62 

WORKDIR /home/atlas
COPY requirements.txt .
RUN source /home/atlas/release_setup.sh; \
    pip install --user -r requirements.txt

# Copy over the source
COPY tools/ .
