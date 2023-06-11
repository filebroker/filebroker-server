# filebroker

Server backend for the filebroker project featuring a warp api server and a docker-compose configuration to host
the api server, the react fronted, the postgres server and the nginx reverse proxy.

To host the filebroker-client frontend it should be installed to `../filebroker-client`.

## Setup

The configuration is managed via the `.env` files, which need to be created in the root directory of this project:

The default `.env` file simply sets the port the API connects with to `8080`:

```text
FILEBROKER_API_PORT=8080
```

The `.env.secret` file contains secret passwords and tokens and must be created with at least the following variables:

```text
FILEBROKER_DATABASE_URL=postgres://postgres:postgres@localhost:5432/filebroker
FILEBROKER_JWT_SECRET=2344654432423
```

You may also create a `.env.local` file to override variables for local development.
Env files are loaded in the following order: 1. `.env.local` 2. `.env.secret` 3. `.env` and the variables present in these
files do not override existing variables, meaning the first occurrence of the variable is applied as long as the variable
does not already exist in the system (e.g. when set via CLI).

The following variables are available:

`FILEBROKER_DATABASE_URL` (mandatory, string): URL of the postgres database, which uses the following format: `postgres://user_name:password@host_name:port/db_name`

`FILEBROKER_JWT_SECRET` (mandatory, u64): Secret unsigned 64 bit integer used to generate JWTs.

`FILEBROKER_API_PORT` (mandatory, u16): Port on which to host the API server, changing this requires adjustments to the nginx configuration in `default.conf`.

`FILEBROKER_MAX_DB_CONNECTIONS` (optional, u32): Maximum size of the DB connection pool, defaults to 25.

`FILEBROKER_CERT_PATH` (optional, string): Path to the SSL cert file for HTTPS.

`FILEBROKER_KEY_PATH` (optional, string): Path to the SSL key file for HTTPS, must be set if `FILEBROKER_CERT_PATH` is set.

`FILEBROKER_API_BASE_URL` (optional, string): Path to the API used for external process that need to access the API, like ffmpeg.
Defaults to `[https|http]://localhost:{FILEBROKER_API_PORT}/` using https if a `FILEBROKER_CERT_PATH` is specified.

`FILEBROKER_CONCURRENT_VIDEO_TRANSCODE_LIMIT` (optional, usize): Maximum number of videos to be transcoded with ffmpeg concurrently.
Defaults to the number of CPUs / 2 or a maximum of 8.

`FILEBROKER_TASK_POOL_WORKER_COUNT` (optional, usize): Number of worker threads in the pool for scheduled background tasks.
Defaults to 4.

`FILEBROKER_DISABLE_GENERATE_MISSING_HLS_STREAMS` (optional, boolean): Disable HLS transcoding for videos. Note that this
only disables the background task that generates the HLS streams for videos where it's missing and helps other servers,
videos uploaded to this server directly will still get transcoded.

`FILEBROKER_DISABLE_GENERATE_MISSING_THUMBNAILS` (optional, boolean): Disable task that generates missing thumbnails in
the background, thumbnails for objects uploaded to this server will still be created.

`FILEBROKER_CAPTCHA_SITE_KEY` (optional, string): The site key for hCaptcha.

`FILEBROKER_CAPTCHA_SECRET` (optional, string): The secret for hCaptcha. If empty then captchas are disabled.

Note that CORS headers are only set when running debug binaries for development, for production you need to set up nginx.
