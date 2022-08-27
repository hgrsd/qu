# Qu

A tiny toy project for a job queue, mostly meant as a means for me to write some Java and Kotlin.

# Setup

- Set up a postgres database if you want to run the tests for the postgres-based queue. You can use the docker-compose file for this, unless you want to use your own local PG instance. In that case, make sure to amend the env vars in `bootstrap.sh`
```sh
docker-compose up -d # set up database
cd bootstrap
./bootstrap.sh # run migration to set up db for qu
```

# Testing

```sh
QU_DBURL=postgresql://127.0.0.1:5432/qu QU_DBPASSWORD=qu QU_DBUSERNAME=qu mvn test
```

# Literature

- https://kerkour.com/rust-job-queue-with-postgresql
- https://shekhargulati.com/2022/01/27/correctly-using-postgres-as-queue/