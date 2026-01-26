#### Virtual ENV
```bash
source .venv/bin/activate
```

#### Command run Postgres in Docker 
-- Step 01: Start Postgres container (Terminal 1) 
```bash
docker run -it \
  -e POSTGRES_USER="root" \
  -e POSTGRES_PASSWORD="root" \
  -e POSTGRES_DB="ny_taxi" \
  -v ny_taxi_postgres_data:/var/lib/postgresql/data \
  -p 5432:5432 \
  --network=pg-network \
  --name pgdatabase \
  postgres:18
```

-- Step 2 — Build your ingest image (Terminal 2)
```bash
cd pipeline
docker build -t taxi_ingest:v001 .
```

-- Step 03: Run ingestion container (Terminal 2)
```bash
docker run -it --rm \
  --network=pg-network \
  taxi_ingest:v001 \
    --pg-user=root \
    --pg-pass=root \
    --pg-host=pgdatabase \
    --pg-port=5432 \
    --pg-db=ny_taxi \
    --target-table=yellow_taxi_trips
```

#### USE cli to connect to postgres
```bash
uv run pgcli -h localhost -p 5432 -u root -d ny_taxi
```

##### RUN PGAdmin Inferface
```bash
docker run -it \
  -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
  -e PGADMIN_DEFAULT_PASSWORD="root" \
  -v pgadmin_data:/var/lib/pgadmin \
  -p 8085:80 \
  --network=pg-network \
  --name pgadmin \
  dpage/pgadmin4
```

### DOCKER C0MPOSE FOR MULTIPLE CONTAINERS
```bash
docker - compose --build --d
```
