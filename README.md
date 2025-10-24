db_fwd
======

Forwards a SQL query result to a web API endpoint.


Usage
-----

```shell
$ ./db_fwd.py \
    --log-level [none,info,debug] \
    --log-file <filename> \
    --config-file <filename> \
    query_name \
    [query_param] [query_param] etc.
```

The log level given on the command line overrides the value set in the
config file. If not specified, the default value is "info".

The log file given on the command line overrides the value set in the
config file. If not specified, the default value is "db_fwd.log".

The config file defaults to "db_fwd.toml".


Environment Variables
---------------------

The use of environment variables is optional. The following are
available:

`DB_FWD_DB_URL` stores the URL of the database to query for all queries.

`DB_FWD_API_USERNAME` stores the username to use for API authentication.
Currently basic auth is the only API authentication method that
**db_fwd** supports.

`DB_FWD_API_PASSWORD` stores the password to use for API authentication.


Configuration File
------------------

The configuration file is given in TOML format. This file can contain
passwords, and should only be readable by the user that **db_fwd** will
run as.

The following is an example configuration:

```toml
[db_fwd]
log_level = 'info'
log_file = 'db_fwd.log'
log_db_url = 'postgresql://username:Passw0rd1@pg.example.com:5432/dbname'

[queries]
db_url = 'postgresql://username:Passw0rd1@pg.example.com:5432/dbname'
api_username = 'admin@example.com'
api_password = 'Password!'

[queries.queryname1]
query = "SELECT json_payload FROM queryname1_view WHERE period = '%s';"
api_url = 'https://dhis2.example.com/api/dataset/abcd1234/'

[queries.queryname2]
query = "SELECT json_payload FROM queryname2_view WHERE category_id = '%s' AND period = '%s';"
api_url = 'https://dhis2.example.com/api/dataset/efgh5678/'
```


### `db_fwd` Section

#### Logging

If not specified, `log_level` defaults to "info". Valid values are
"none", "info" and "debug".

If not specified, `log_file` defaults to "db_fwd.log" in the same
directory as `db_fwd.py`.

`log_db_url` is optional. It stores a database URL. If it is specified
then debug-level logs will be stored in the "db_fwd_logs" table. (Log
level is _always debug_ regardless of the `log_level` setting used for
`log_file`. This is so that request and response details are available.)


### `queries` Section

`db_url` is the URL of the database to query for all queries. This
setting overrides the environment variable "DB_FWD_DB_URL". A query can
override this value by specifying `db_url` in its section.

`api_url` can be used to specify the URL that all query results are
forwarded to. This setting is optional. A query can override this value
by specifying `api_url` in its section.

`api_username` sets the username, and `api_password` override the
environment variables "DB_FWD_API_USERNAME" and "DB_FWD_API_PASSWORD".


### `queries.queryname` Sections

Each query has a section.

`query` is the SQL query that will be executed. It must return a single
field.

`api_url` is the API endpoint that the value of the field returned by
`query` will be forwarded to. If `api_url` was given in the `queries`
section then this setting is optional. If both are given then this value
overrides the value given in the `queries` section.

`api_username` and `api_password` can optionally be set here to override
the values given in the `queries` section.
