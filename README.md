# filebroker

Server backend for the filebroker project featuring a warp api server and a docker-compose configuration to host
the api server, the react fronted, the postgres server and the nginx reverse proxy.

To host the filebroker-client frontend it should be installed to `../filebroker-client`.

## Dependencies

* Requires [ffmpeg](https://trac.ffmpeg.org/wiki/CompilationGuide) to generate thumbnails and transcode media, make sure to enable webp using `--enable-libwebp`
* Requires [exiftool](https://exiftool.org/) for file metadata extraction

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

`FILEBROKER_DISABLE_LOAD_MISSING_METADATA` (optional, boolean): Disable task that loads missing s3 object metadata.

`FILEBROKER_CAPTCHA_SITE_KEY` (optional, string): The site key for hCaptcha.

`FILEBROKER_CAPTCHA_SECRET` (optional, string): The secret for hCaptcha. If empty then captchas are disabled.

`FILEBROKER_HOST_BASE_PATH` (optional, string): Base path for the host, used to build links for email confirmation links etc. Uses the Host header of the request if absent.

`FILEBROKER_MAIL_SENDER_NAME` (optional, string): The name for the mail sender address, defaults to "filebroker".

`FILEBROKER_MAIL_SENDER_ADDRESS` (optional, string): Email address to send emails from, required to send mails.

`FILEBROKER_SMTP_HOST` (optional, string): The host name for the SMTP service to send mails through, required to send mails.

`FILEBROKER_SMTP_USER` (optional, string): The username to authenticate to the SMTP host with, required to send mails.

`FILEBROKER_SMTP_PASSWORD` (optional, string): The password to authenticate to the SMTP host with, required to send mails.

`FILEBROKER_MAILS_PER_HOUR_LIMIT` (optional, u32): The rate limit for maximum number of mails sent per hour, defaults to 120.

`FILEBROKER_DKIM_KEY_PATH` (optional, string): Path to the DKIM private key to sign emails with. Only necessary if you manage DKIM yourself,
if you send your mails through an SMTP relay that manages DKIM for you this is redundant.

`FILEBROKER_DKIM_SELECTOR` (optional, string): the DKIM selector for the DNS entry, used in combination with `FILEBROKER_DKIM_KEY_PATH`.

`FILEBROKER_DKIM_DOMAIN` (optional, string): The domain name verified through the DKIM entry, used in combination with `FILEBROKER_DKIM_KEY_PATH` and `FILEBROKER_DKIM_SELECTOR`.

`FILEBROKER_PG_ENABLE_SSL` (optional, boolean): Whether to enable SSL for postgres connection, defaults to false.

`FILEBROKER_PG_SSL_CERT_PATH` (optional, string): Path to the SSL certificate used for postgres connections, used in addition to the native certificate store. Meaningless if `FILEBROKER_PG_ENABLE_SSL` is not enabled.

Note that CORS headers are only set when running debug binaries for development, for production you need to set up nginx.
