<h2 align="center">

![](images/logo-tobiko-cloud.svg)

</h2>

This is a simple, loveable, and complete Tobiko Cloud demo project with the goal of running multiple, realistic scenarios very fast. 

This repo is going to try something a bit novel compared to your standard demo projects. I'll take you through different stories that illustrate the data engineering workflow. It'll engage your inner dialogue along with look and feel of the development experience. The hope is for you to better internalize and translate these stories to your own reality. Don't worry, these stories won't be too long-winded!

This is intentionally similar to what people may have experienced when I created this github repo at dbt Labs: [here](https://github.com/dbt-labs/jaffle_shop_duckdb)


## Basic Setup

What you'll be using:

- Tobiko Cloud: state backend (uses postgres) and observability
- SQLMesh: transformation framework
- SQLGlot: Python macros that compile to SQL
- pytest: test Python macros
- BigQuery: data warehouse to run transforms against
- DuckDB: local development testing

<details>

**Setup your virtual environment for SQLMesh:**

```bash
git clone https://github.com/TobikoData/tobiko-cloud-demo.git # clone the repo
cd tobiko-cloud-demo # go to the root directory
make dev-install # virtual environment setup
```

**Setup your BigQuery Service Account:**

![service_account](./images/bigquery_service_account.png)

1. Create a service account following these instructions: [here](https://cloud.google.com/iam/docs/service-accounts-create)
2. Add permissions: `BigQuery Data Editor`, `BigQuery User`
3. Download the service account json file
4. Copy the contents of the service account file to your clipboard
5. Export the credentials as an environment variable in your terminal: 

`export GOOGLE_SQLMESH_CREDENTIALS=<your-service-account-key-contents>`

**Setup your Tobiko Cloud State Connection:**

1. Work with Tobiko to get your Tobiko Cloud Token and account url

```bash
# examples based on the image above
export TOBIKO_CLOUD_TOKEN=<TOBIKO_CLOUD_TOKEN>
```

```yaml
# config.yaml gateway example
gateways:
    tobiko_cloud:
        connection:
            type: bigquery
            method: service-account-json
            concurrent_tasks: 5
            register_comments: true
            keyfile_json: {{ env_var('GOOGLE_SQLMESH_CREDENTIALS') }}
            project: sqlmesh-public-demo # TODO: update this
        state_connection:
            type: cloud
            url: https://sqlmesh-prod-enterprise-public-demo-sefz6ezt4q-uc.a.run.app # TODO: replace this url with your own
            token: "{{ env_var('TOBIKO_CLOUD_TOKEN') }}"
```

**Verify SQLMesh can connect to BigQuery and Tobiko Cloud:**

```bash
sqlmesh info # print info about a SQLMesh project

# expected output
Models: 15
Macros: 1
Data warehouse connection succeeded
State backend connection succeeded
Test connection succeeded
```

</details>

## Story #1

How do I run SQLMesh as fast as possible and get the general look and feel?

Run this command!

```bash
sqlmesh plan # follow the instructions in the CLI prompt
```

More details on the demo video will be added soon!


### Credits

Portions of this project are modifications based on work created and shared by dbt Labs and used according to terms described in the Apache License, Version 2.0. For the original work and its license, see: [here](https://github.com/dbt-labs/jaffle_shop_duckdb?tab=Apache-2.0-1-ov-file#readme)
