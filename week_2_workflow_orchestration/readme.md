### Command to run Prefect

```
prefect orion start
```

### [Yellow NY Taxi Data](https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/yellow)

### prefect command

1. deploy

```
prefect deployment build flows/01_start/parameterized_flow.py:etl_parent_flow -n "Parameterized ETL"

prefect deployment build flows/03_deployments/parameterized_flow.py:etl_parent_flow -n etl2 --cron "0 0 * * *" -a
```

2. edit parameter

```
edit your parameter in yaml file
```

3. running agent

```
prefect agent start -q 'default'
```

### build prefect docker

```
docker image build -t discdiver/prefect:zoom .
docker push oillypump/prefect:zoom
```
