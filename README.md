# filebroker

Server backend for the filebroker project featuring a warp api server and a docker-compose configuration to host
the api server, the react fronted, the postgres server and the nginx reverse proxy.

To host the filebroker-client frontend it should be installed to `../filebroker-client`.

## Setup

The configuration is managed via the `.env` file, which needs to be created in the root directory of this project:
```text
DATABASE_URL=postgres://postgres_user:postgres_password@filebroker-db:5432/filebroker
JWT_SECRET=234463534432423
API_PORT=8080
POSTGRES_USER=postgres_user
POSTGRES_PASSWORD=postgres_password
POSTGRES_DB=filebroker
```
`DATABASE_URL`: URL of the postgres database, note that user and password should match `POSTGRES_USER` and `POSTGRES_PASSWORD`
and the hostname `filebroker-db` should be changed to `localhost` if hosted locally instead of through Docker.

`JWT_SECRET`: Secret unsigned 64 bit integer used to generate JWTs.

`API_PORT`: Port on which to host the API server, changing this requires adjustments to the nginx configuration in `default.conf`.

`POSTGRES_USER` and `POSTGRES_USER`: User and password for postgres.

`POSTGRES_DB`: Name of the postgres database.

Note that CORS headers are only set when running debug binaries for development, for production you need to set up nginx.
