docker pull timescale/timescaledb-ha:pg17
docker run -d --name timescaledb -p 5432:5432 -e POSTGRES_PASSWORD=123456789 timescale/timescaledb-ha:pg17