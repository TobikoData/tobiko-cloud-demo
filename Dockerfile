FROM quay.io/astronomer/astro-runtime:12.5.0

# Copy the local directories into the container

COPY ./sqlmesh-enterprise/sqlmesh_enterprise /usr/local/lib/python3.12/site-packages/sqlmesh_enterprise
COPY ./sqlmesh-enterprise/services /usr/local/lib/python3.12/site-packages/services
COPY ./sqlmesh-enterprise/api_models /usr/local/lib/python3.12/site-packages/api_models
COPY ./sqlmesh-enterprise/tobikodata /usr/local/lib/python3.12/site-packages/tobikodata