# fluent-plugin-bigquery

[Fluentd](http://fluentd.org) output plugin to load/insert data into Google BigQuery.

- **Plugin type**: Output

* insert data over streaming inserts
  * plugin type is `bigquery_insert`
  * for continuous real-time insertions
  * https://developers.google.com/bigquery/streaming-data-into-bigquery#usecases
* load data
  * plugin type is `bigquery_load`
  * for data loading as batch jobs, for big amount of data
  * https://developers.google.com/bigquery/loading-data-into-bigquery

Current version of this plugin supports Google API with Service Account Authentication, but does not support
OAuth flow for installed applications.

## Support Version

| plugin version | fluentd version | ruby version |
| :-----------   | :-----------    | :----------- |
| v0.4.x         | 0.12.x          | 2.0 or later |
| v1.x.x         | 0.14.x or later | 2.2 or later |
| v2.x.x         | 0.14.x or later | 2.3 or later |
| v3.x.x         | 1.x or later    | 2.7 or later |

## With docker image
If you use official alpine based fluentd docker image (https://github.com/fluent/fluentd-docker-image),
You need to install `bigdecimal` gem on your own dockerfile.
Because alpine based image has only minimal ruby environment in order to reduce image size.
And in most case, dependency to embedded gem is not written on gemspec.
Because embbeded gem dependency sometimes restricts ruby environment.

## Configuration

### Options

#### common

| name                                          | type          | required?                                    | placeholder? | default                    | description                                                                                            |
| :-------------------------------------------- | :------------ | :-----------                                 | :----------  | :------------------------- | :-----------------------                                                                               |
| auth_method                                   | enum          | yes                                          | no           | private_key                | `private_key` or `json_key` or `compute_engine` or `application_default`                               |
| email                                         | string        | yes (private_key)                            | no           | nil                        | GCP Service Account Email                                                                              |
| private_key_path                              | string        | yes (private_key)                            | no           | nil                        | GCP Private Key file path                                                                              |
| private_key_passphrase                        | string        | yes (private_key)                            | no           | nil                        | GCP Private Key Passphrase                                                                             |
| json_key                                      | string        | yes (json_key)                               | no           | nil                        | GCP JSON Key file path or JSON Key string                                                              |
| location                                      | string        | no                                           | no           | nil                        | BigQuery Data Location. The geographic location of the job. Required except for US and EU.             |
| project                                       | string        | yes                                          | yes          | nil                        |                                                                                                        |
| dataset                                       | string        | yes                                          | yes          | nil                        |                                                                                                        |
| table                                         | string        | yes (either `tables`)                        | yes          | nil                        |                                                                                                        |
| tables                                        | array(string) | yes (either `table`)                         | yes          | nil                        | can set multi table names splitted by `,`                                                              |
| auto_create_table                             | bool          | no                                           | no           | false                      | If true, creates table automatically                                                                   |
| ignore_unknown_values                         | bool          | no                                           | no           | false                      | Accept rows that contain values that do not match the schema. The unknown values are ignored.          |
| schema                                        | array         | yes (either `fetch_schema` or `schema_path`) | no           | nil                        | Schema Definition. It is formatted by JSON.                                                            |
| schema_path                                   | string        | yes (either `fetch_schema`)                  | yes          | nil                        | Schema Definition file path. It is formatted by JSON.                                                  |
| fetch_schema                                  | bool          | yes (either `schema_path`)                   | no           | false                      | If true, fetch table schema definition from Bigquery table automatically.                              |
| fetch_schema_table                            | string        | no                                           | yes          | nil                        | If set, fetch table schema definition from this table, If fetch_schema is false, this param is ignored |
| schema_cache_expire                           | integer       | no                                           | no           | 600                        | Value is second. If current time is after expiration interval, re-fetch table schema definition.       |
| request_timeout_sec                           | integer       | no                                           | no           | nil                        | Bigquery API response timeout                                                                          |
| request_open_timeout_sec                      | integer       | no                                           | no           | 60                         | Bigquery API connection, and request timeout. If you send big data to Bigquery, set large value.       |
| time_partitioning_type                        | enum          | no (either day)                              | no           | nil                        | Type of bigquery time partitioning feature.                                                            |
| time_partitioning_field                       | string        | no                                           | no           | nil                        | Field used to determine how to create a time-based partition.                                          |
| time_partitioning_expiration                  | time          | no                                           | no           | nil                        | Expiration milliseconds for bigquery time partitioning.                                                |
| clustering_fields                             | array(string) | no                                           | no           | nil                        | One or more fields on which data should be clustered. The order of the specified columns determines the sort order of the data. |

#### bigquery_insert

| name                                   | type          | required?    | placeholder? | default                    | description                                                                                                                                                                                |
| :------------------------------------- | :------------ | :----------- | :----------  | :------------------------- | :-----------------------                                                                                                                                                                   |
| template_suffix                        | string        | no           | yes          | nil                        | can use `%{time_slice}` placeholder replaced by `time_slice_format`                                                                                                                        |
| skip_invalid_rows                      | bool          | no           | no           | false                      |                                                                                                                                                                                            |
| insert_id_field                        | string        | no           | no           | nil                        | Use key as `insert_id` of Streaming Insert API parameter. see. https://docs.fluentd.org/v1.0/articles/api-plugin-helper-record_accessor                                                    |
| add_insert_timestamp                   | string        | no           | no           | nil                        | Adds a timestamp column just before sending the rows to BigQuery, so that buffering time is not taken into account. Gives a field in BigQuery which represents the insert time of the row. |
| allow_retry_insert_errors              | bool          | no           | no           | false                      | Retry to insert rows when an insertErrors occurs. There is a possibility that rows are inserted in duplicate.                                                                              |
| require_partition_filter    | bool          | no                                           | no           | false                      | If true, queries over this table require a partition filter that can be used for partition elimination to be specified. |

#### bigquery_load

| name                                   | type          | required?    | placeholder? | default                    | description                                                                                                                                    |
| :------------------------------------- | :------------ | :----------- | :----------  | :------------------------- | :-----------------------                                                                                                                       |
| source_format                          | enum          | no           | no           | json                       | Specify source format `json` or `csv` or `avro`. If you change this parameter, you must change formatter plugin via `<format>` config section. |
| max_bad_records                        | integer       | no           | no           | 0                          | If the number of bad records exceeds this value, an invalid error is returned in the job result.                                               |

### Buffer section

| name                                   | type          | required?    | default                        | description                        |
| :------------------------------------- | :------------ | :----------- | :-------------------------     | :-----------------------           |
| @type                                  | string        | no           | memory (insert) or file (load) |                                    |
| chunk_limit_size                       | integer       | no           | 1MB (insert) or 1GB (load)     |                                    |
| total_limit_size                       | integer       | no           | 1GB (insert) or 32GB (load)    |                                    |
| chunk_records_limit                    | integer       | no           | 500 (insert) or nil (load)     |                                    |
| flush_mode                             | enum          | no           | interval                       | default, lazy, interval, immediate |
| flush_interval                         | float         | no           | 1.0 (insert) or 3600 (load)    |                                    |
| flush_thread_interval                  | float         | no           | 0.05 (insert) or 5 (load)      |                                    |
| flush_thread_burst_interval            | float         | no           | 0.05 (insert) or 5 (load)      |                                    |

And, other params (defined by base class) are available

see. https://github.com/fluent/fluentd/blob/master/lib/fluent/plugin/output.rb

### Inject section

It is replacement of previous version `time_field` and `time_format`.

For example.

```
<inject>
  time_key time_field_name
  time_type string
  time_format %Y-%m-%d %H:%M:%S
</inject>
```

| name                                   | type          | required?    | default                    | description              |
| :------------------------------------- | :------------ | :----------- | :------------------------- | :----------------------- |
| hostname_key                           | string        | no           | nil                        |                          |
| hostname                               | string        | no           | nil                        |                          |
| tag_key                                | string        | no           | nil                        |                          |
| time_key                               | string        | no           | nil                        |                          |
| time_type                              | string        | no           | nil                        |                          |
| time_format                            | string        | no           | nil                        |                          |
| localtime                              | bool          | no           | true                       |                          |
| utc                                    | bool          | no           | false                      |                          |
| timezone                               | string        | no           | nil                        |                          |

see. https://github.com/fluent/fluentd/blob/master/lib/fluent/plugin_helper/inject.rb

### Formatter section

This section is for `load` mode only.
If you use `insert` mode, used formatter is `json` only.

Bigquery supports `csv`, `json` and `avro` format. Default is `json`
I recommend to use `json` for now.

For example.

```
source_format csv

<format>
  @type csv
  fields col1, col2, col3
</format>
```

see. https://github.com/fluent/fluentd/blob/master/lib/fluent/plugin_helper/formatter.rb

## Examples

### Streaming inserts

Configure insert specifications with target table schema, with your credentials. This is minimum configurations:

```apache
<match dummy>
  @type bigquery_insert

  auth_method private_key   # default
  email xxxxxxxxxxxx-xxxxxxxxxxxxxxxxxxxxxx@developer.gserviceaccount.com
  private_key_path /home/username/.keys/00000000000000000000000000000000-privatekey.p12
  # private_key_passphrase notasecret # default

  project yourproject_id
  dataset yourdataset_id
  table   tablename

  schema [
    {"name": "time", "type": "INTEGER"},
    {"name": "status", "type": "INTEGER"},
    {"name": "bytes", "type": "INTEGER"},
    {"name": "vhost", "type": "STRING"},
    {"name": "path", "type": "STRING"},
    {"name": "method", "type": "STRING"},
    {"name": "protocol", "type": "STRING"},
    {"name": "agent", "type": "STRING"},
    {"name": "referer", "type": "STRING"},
    {"name": "remote", "type": "RECORD", "fields": [
      {"name": "host", "type": "STRING"},
      {"name": "ip", "type": "STRING"},
      {"name": "user", "type": "STRING"}
    ]},
    {"name": "requesttime", "type": "FLOAT"},
    {"name": "bot_access", "type": "BOOLEAN"},
    {"name": "loginsession", "type": "BOOLEAN"}
  ]
</match>
```

For high rate inserts over streaming inserts, you should specify flush intervals and buffer chunk options:

```apache
<match dummy>
  @type bigquery_insert
  
  <buffer>
    flush_interval 0.1  # flush as frequent as possible
    
    total_limit_size 10g
    
    flush_thread_count 16
  </buffer>
  
  auth_method private_key   # default
  email xxxxxxxxxxxx-xxxxxxxxxxxxxxxxxxxxxx@developer.gserviceaccount.com
  private_key_path /home/username/.keys/00000000000000000000000000000000-privatekey.p12
  # private_key_passphrase notasecret # default

  project yourproject_id
  dataset yourdataset_id
  tables  accesslog1,accesslog2,accesslog3

  schema [
    {"name": "time", "type": "INTEGER"},
    {"name": "status", "type": "INTEGER"},
    {"name": "bytes", "type": "INTEGER"},
    {"name": "vhost", "type": "STRING"},
    {"name": "path", "type": "STRING"},
    {"name": "method", "type": "STRING"},
    {"name": "protocol", "type": "STRING"},
    {"name": "agent", "type": "STRING"},
    {"name": "referer", "type": "STRING"},
    {"name": "remote", "type": "RECORD", "fields": [
      {"name": "host", "type": "STRING"},
      {"name": "ip", "type": "STRING"},
      {"name": "user", "type": "STRING"}
    ]},
    {"name": "requesttime", "type": "FLOAT"},
    {"name": "bot_access", "type": "BOOLEAN"},
    {"name": "loginsession", "type": "BOOLEAN"}
  ]
</match>
```

Important options for high rate events are:

  * `tables`
    * 2 or more tables are available with ',' separator
    * `out_bigquery` uses these tables for Table Sharding inserts
    * these must have same schema
  * `buffer/chunk_limit_size`
    * max size of an insert or chunk (default 1000000 or 1MB)
    * the max size is limited to 1MB on BigQuery
  * `buffer/chunk_records_limit`
    * number of records over streaming inserts API call is limited as 500, per insert or chunk
    * `out_bigquery` flushes buffer with 500 records for 1 inserts API call
  * `buffer/queue_length_limit`
    * BigQuery streaming inserts needs very small buffer chunks
    * for high-rate events, `buffer_queue_limit` should be configured with big number
    * Max 1GB memory may be used under network problem in default configuration
      * `chunk_limit_size (default 1MB)` x `queue_length_limit (default 1024)`
  * `buffer/flush_thread_count`
    * threads for insert api calls in parallel
    * specify this option for 100 or more records per seconds
    * 10 or more threads seems good for inserts over internet
    * less threads may be good for Google Compute Engine instances (with low latency for BigQuery)
  * `buffer/flush_interval`
    * interval between data flushes (default 0.25)
    * you can set subsecond values such as `0.15` on Fluentd v0.10.42 or later

See [Quota policy](https://cloud.google.com/bigquery/streaming-data-into-bigquery#quota)
section in the Google BigQuery document.

### Load
```apache
<match bigquery>
  @type bigquery_load

  <buffer>
    path bigquery.*.buffer
    flush_at_shutdown true
    timekey_use_utc
  </buffer>

  auth_method json_key
  json_key json_key_path.json

  project yourproject_id
  dataset yourdataset_id
  auto_create_table true
  table yourtable%{time_slice}
  schema_path bq_schema.json
</match>
```

I recommend to use file buffer and long flush interval.

### Authentication

There are four methods supported to fetch access token for the service account.

1. Public-Private key pair of GCP(Google Cloud Platform)'s service account
2. JSON key of GCP(Google Cloud Platform)'s service account
3. Predefined access token (Compute Engine only)
4. Google application default credentials (http://goo.gl/IUuyuX)

#### Public-Private key pair of GCP's service account

The examples above use the first one. You first need to create a service account (client ID),
download its private key and deploy the key with fluentd.

#### JSON key of GCP(Google Cloud Platform)'s service account

You first need to create a service account (client ID),
download its JSON key and deploy the key with fluentd.

```apache
<match dummy>
  @type bigquery_insert

  auth_method json_key
  json_key /home/username/.keys/00000000000000000000000000000000-jsonkey.json

  project yourproject_id
  dataset yourdataset_id
  table   tablename
  ...
</match>
```

You can also provide `json_key` as embedded JSON string like this.
You need to only include `private_key` and `client_email` key from JSON key file.

```apache
<match dummy>
  @type bigquery_insert

  auth_method json_key
  json_key {"private_key": "-----BEGIN PRIVATE KEY-----\n...", "client_email": "xxx@developer.gserviceaccount.com"}

  project yourproject_id
  dataset yourdataset_id
  table   tablename
  ...
</match>
```

#### Predefined access token (Compute Engine only)

When you run fluentd on Googlce Compute Engine instance,
you don't need to explicitly create a service account for fluentd.
In this authentication method, you need to add the API scope "https://www.googleapis.com/auth/bigquery" to the scope list of your
Compute Engine instance, then you can configure fluentd like this.

```apache
<match dummy>
  @type bigquery_insert

  auth_method compute_engine

  project yourproject_id
  dataset yourdataset_id
  table   tablename

  ...
</match>
```

#### Application default credentials

The Application Default Credentials provide a simple way to get authorization credentials for use in calling Google APIs, which are described in detail at http://goo.gl/IUuyuX.

In this authentication method, the credentials returned are determined by the environment the code is running in. Conditions are checked in the following order:credentials are get from following order.

1. The environment variable `GOOGLE_APPLICATION_CREDENTIALS` is checked. If this variable is specified it should point to a JSON key file that defines the credentials.
2. The environment variable `GOOGLE_PRIVATE_KEY` and `GOOGLE_CLIENT_EMAIL` are checked. If this variables are specified `GOOGLE_PRIVATE_KEY` should point to `private_key`, `GOOGLE_CLIENT_EMAIL` should point to `client_email` in a JSON key.
3. Well known path is checked. If file is exists, the file used as a JSON key file. This path is `$HOME/.config/gcloud/application_default_credentials.json`.
4. System default path is checked. If file is exists, the file used as a JSON key file. This path is `/etc/google/auth/application_default_credentials.json`.
5. If you are running in Google Compute Engine production, the built-in service account associated with the virtual machine instance will be used.
6. If none of these conditions is true, an error will occur.

### Table id formatting

this plugin supports fluentd-0.14 style placeholder.

#### strftime formatting
`table` and `tables` options accept [Time#strftime](http://ruby-doc.org/core-1.9.3/Time.html#method-i-strftime)
format to construct table ids.
Table ids are formatted at runtime
using the chunk key time.

see. https://docs.fluentd.org/configuration/buffer-section

For example, with the configuration below,
data is inserted into tables `accesslog_2014_08_02`, `accesslog_2014_08_03` and so on.

```apache
<match dummy>
  @type bigquery_insert

  ...

  project yourproject_id
  dataset yourdataset_id
  table   accesslog_%Y_%m_%d

  <buffer time>
    timekey 1d
  </buffer>
  ...
</match>
```

**NOTE: In current fluentd (v1.15.x), The maximum unit supported by strftime formatting is the granularity of days**

#### record attribute formatting
The format can be suffixed with attribute name.

__CAUTION: format is different with previous version__

```apache
<match dummy>
  ...
  table   accesslog_${status_code}

  <buffer status_code>
  </buffer>
  ...
</match>
```

If attribute name is given, the time to be used for formatting is value of each row.
The value for the time should be a UNIX time.

#### time_slice_key formatting

Instead, Use strftime formatting.

strftime formatting of current version is based on chunk key.
That is same with previous time_slice_key formatting .

### Date partitioned table support
this plugin can insert (load) into date partitioned table.

Use placeholder.

```apache
<match dummy>
  @type bigquery_load

  ...
  table   accesslog$%Y%m%d

  <buffer time>
    timekey 1d
  </buffer>
  ...
</match>
```

But, Dynamic table creating doesn't support date partitioned table yet.
And streaming insert is not allowed to insert with `$%Y%m%d` suffix.
If you use date partitioned table with streaming insert, Please omit `$%Y%m%d` suffix from `table`.

### Dynamic table creating

When `auto_create_table` is set to `true`, try to create the table using BigQuery API when insertion failed with code=404 "Not Found: Table ...".
Next retry of insertion is expected to be success.

NOTE: `auto_create_table` option cannot be used with `fetch_schema`. You should create the table on ahead to use `fetch_schema`.

```apache
<match dummy>
  @type bigquery_insert

  ...

  auto_create_table true
  table accesslog_%Y_%m

  ...
</match>
```

Also, you can create clustered table by using `clustering_fields`.

### Table schema

There are three methods to describe the schema of the target table.

1. List fields in fluent.conf
2. Load a schema file in JSON.
3. Fetch a schema using BigQuery API

The examples above use the first method.  In this method,
you can also specify nested fields by prefixing their belonging record fields.

```apache
<match dummy>
  @type bigquery_insert

  ...

  schema [
    {"name": "time", "type": "INTEGER"},
    {"name": "status", "type": "INTEGER"},
    {"name": "bytes", "type": "INTEGER"},
    {"name": "vhost", "type": "STRING"},
    {"name": "path", "type": "STRING"},
    {"name": "method", "type": "STRING"},
    {"name": "protocol", "type": "STRING"},
    {"name": "agent", "type": "STRING"},
    {"name": "referer", "type": "STRING"},
    {"name": "remote", "type": "RECORD", "fields": [
      {"name": "host", "type": "STRING"},
      {"name": "ip", "type": "STRING"},
      {"name": "user", "type": "STRING"}
    ]},
    {"name": "requesttime", "type": "FLOAT"},
    {"name": "bot_access", "type": "BOOLEAN"},
    {"name": "loginsession", "type": "BOOLEAN"}
  ]
</match>
```

This schema accepts structured JSON data like:

```json
{
  "request":{
    "time":1391748126.7000976,
    "vhost":"www.example.com",
    "path":"/",
    "method":"GET",
    "protocol":"HTTP/1.1",
    "agent":"HotJava",
    "bot_access":false
  },
  "remote":{ "ip": "192.0.2.1" },
  "response":{
    "status":200,
    "bytes":1024
  }
}
```

The second method is to specify a path to a BigQuery schema file instead of listing fields.  In this case, your fluent.conf looks like:

```apache
<match dummy>
  @type bigquery_insert

  ...
  
  schema_path /path/to/httpd.schema
</match>
```
where /path/to/httpd.schema is a path to the JSON-encoded schema file which you used for creating the table on BigQuery. By using external schema file you are able to write full schema that does support NULLABLE/REQUIRED/REPEATED, this feature is really useful and adds full flexbility.

The third method is to set `fetch_schema` to `true` to enable fetch a schema using BigQuery API.  In this case, your fluent.conf looks like:

```apache
<match dummy>
  @type bigquery_insert

  ...
  
  fetch_schema true
  # fetch_schema_table other_table # if you want to fetch schema from other table
</match>
```

If you specify multiple tables in configuration file, plugin get all schema data from BigQuery and merge it.

NOTE: Since JSON does not define how to encode data of TIMESTAMP type,
you are still recommended to specify JSON types for TIMESTAMP fields as "time" field does in the example, if you use second or third method.

### Specifying insertId property

BigQuery uses `insertId` property to detect duplicate insertion requests (see [data consistency](https://cloud.google.com/bigquery/streaming-data-into-bigquery#dataconsistency) in Google BigQuery documents).
You can set `insert_id_field` option to specify the field to use as `insertId` property.
`insert_id_field` can use fluentd record_accessor format like `$['key1'][0]['key2']`.
(detail. https://docs.fluentd.org/v1.0/articles/api-plugin-helper-record_accessor)

```apache
<match dummy>
  @type bigquery_insert

  ...

  insert_id_field uuid
  schema [{"name": "uuid", "type": "STRING"}]
</match>
```

## TODO

* OAuth installed application credentials support
* Google API discovery expiration
* check row size limits

## Authors

* @tagomoris: First author, original version
* KAIZEN platform Inc.: Maintener, Since 2014.08.19
* @joker1007
