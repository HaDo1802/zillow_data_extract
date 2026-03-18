-- create airflow db (if you need it for Airflow metadata)
CREATE DATABASE airflow_db;
GRANT ALL PRIVILEGES ON DATABASE airflow_db TO db_user;
-- `docker_real_estate` is created automatically by the postgres image because
-- POSTGRES_DB is set in docker-compose/.env.
GRANT ALL PRIVILEGES ON DATABASE docker_real_estate TO db_user;
