# fluent-plugin-bigquery

[Fluentd](http://fluentd.org) output plugin to load/insert data into Google BigQuery.

- **Plugin type**: TimeSlicedOutput

* insert data over streaming inserts
  * for continuous real-time insertions
  * https://developers.google.com/bigquery/streaming-data-into-bigquery#usecases
* load data
  * for data loading as batch jobs, for big amount of data
  * https://developers.google.com/bigquery/loading-data-into-bigquery

Current version of this plugin supports Google API with Service Account Authentication, but does not support
OAuth flow for installed applications.

## Configuration

### Options

| name                                   | type          | required?                   | default                                                | description                                                                                                          |
| :------------------------------------- | :------------ | :-----------                | :-------------------------                             | :-----------------------                                                                                             |
| method                                 | string        | no                          | insert                                                 | `insert` (Streaming Insert) or `load` (load job)                                                                     |
| auth_method                            | enum          | yes                         | private_key                                            | `private_key` or `json_key` or `compute_engine` or `application_default`                                             |
| email                                  | string        | yes (private_key)           | nil                                                    | GCP Service Account Email                                                                                            |
| private_key_path                       | string        | yes (private_key)           | nil                                                    | GCP Private Key file path                                                                                            |
| private_key_passphrase                 | string        | yes (private_key)           | nil                                                    | GCP Private Key Passphrase                                                                                           |
| json_key                               | string        | yes (json_key)              | nil                                                    | GCP JSON Key file path or JSON Key string                                                                            |
| project                                | string        | yes                         | nil                                                    |                                                                                                                      |
| table                                  | string        | yes (either `tables`)       | nil                                                    |                                                                                                                      |
| tables                                 | string        | yes (either `table`)        | nil                                                    | can set multi table names splitted by `,`                                                                            |
| template_suffix                        | string        | no                          | nil                                                    | can use `%{time_slice}` placeholder replaced by `time_slice_format`                                                  |
| auto_create_table                      | bool          | no                          | false                                                  | If true, creates table automatically                                                                                 |
| skip_invalid_rows                      | bool          | no                          | false                                                  | Only `insert` method.                                                                                                |
| max_bad_records                        | integer       | no                          | 0                                                      | Only `load` method. If the number of bad records exceeds this value, an invalid error is returned in the job result. |
| ignore_unknown_values                  | bool          | no                          | false                                                  | Accept rows that contain values that do not match the schema. The unknown values are ignored.                        |
| schema_path                            | string        | yes (either `fetch_schema`) | nil                                                    | Schema Definition file path. It is formatted by JSON.                                                                |
| fetch_schema                           | bool          | yes (either `schema_path`)  | false                                                  | If true, fetch table schema definition from Bigquery table automatically.                                            |
| fetch_schema_table                     | string        | no                          | nil                                                    | If set, fetch table schema definition from this table, If fetch_schema is false, this param is ignored               |
| schema_cache_expire                    | integer       | no                          | 600                                                    | Value is second. If current time is after expiration interval, re-fetch table schema definition.                     |
| field_string                           | string        | no                          | nil                                                    | see examples.                                                                                                        |
| field_integer                          | string        | no                          | nil                                                    | see examples.                                                                                                        |
| field_float                            | string        | no                          | nil                                                    | see examples.                                                                                                        |
| field_boolean                          | string        | no                          | nil                                                    | see examples.                                                                                                        |
| field_timestamp                        | string        | no                          | nil                                                    | see examples.                                                                                                        |
| replace_record_key                     | bool          | no                          | false                                                  | see examples.                                                                                                        |
| replace_record_key_regexp{1-10}        | string        | no                          | nil                                                    | see examples.                                                                                                        |
| convert_hash_to_json                   | bool          | no                          | false                                                  | If true, converts Hash value of record to JSON String.                                                               |
| insert_id_field                        | string        | no                          | nil                                                    | Use key as `insert_id` of Streaming Insert API parameter.                                                            |
| request_timeout_sec                    | integer       | no                          | nil                                                    | Bigquery API response timeout                                                                                        |
| request_open_timeout_sec               | integer       | no                          | 60                                                     | Bigquery API connection, and request timeout. If you send big data to Bigquery, set large value.                     |
| time_partitioning_type                 | enum          | no (either day)             | nil                                                    | Type of bigquery time partitioning feature(experimental feature on BigQuery).                                        |
| time_partitioning_expiration           | time          | no                          | nil                                                    | Expiration milliseconds for bigquery time partitioning. (experimental feature on BigQuery)                           |

### Buffer section

| name                                   | type          | required?    | default                        | description                        |
| :------------------------------------- | :------------ | :----------- | :-------------------------     | :-----------------------           |
| @type                                  | string        | no           | memory (insert) or file (load) |                                    |
| chunk_limit_size                       | integer       | no           | 1MB (insert) or 1GB (load)     |                                    |
| total_limit_size                       | integer       | no           | 1GB (insert) or 32GB (load)    |                                    |
| chunk_records_limit                    | integer       | no           | 500 (insert) or nil (load)     |                                    |
| flush_mode                             | enum          | no           | interval                       | default, lazy, interval, immediate |
| flush_interval                         | float         | no           | 0.25 (insert) or nil (load)    |                                    |
| flush_thread_interval                  | float         | no           | 0.05 (insert) or nil (load)    |                                    |
| flush_thread_burst_interval            | float         | no           | 0.05 (insert) or nil (load)    |                                    |

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

## Examples

### Streaming inserts

Configure insert specifications with target table schema, with your credentials. This is minimum configurations:

```apache
<match dummy>
  @type bigquery

  method insert    # default

  auth_method private_key   # default
  email xxxxxxxxxxxx-xxxxxxxxxxxxxxxxxxxxxx@developer.gserviceaccount.com
  private_key_path /home/username/.keys/00000000000000000000000000000000-privatekey.p12
  # private_key_passphrase notasecret # default

  project yourproject_id
  dataset yourdataset_id
  table   tablename
  
  field_integer time,status,bytes
  field_string  rhost,vhost,path,method,protocol,agent,referer
  field_float   requesttime
  field_boolean bot_access,loginsession
</match>
```

For high rate inserts over streaming inserts, you should specify flush intervals and buffer chunk options:

```apache
<match dummy>
  @type bigquery

  method insert    # default
  
  <buffer>
    flush_interval 0.1  # flush as frequent as possible
    
    buffer_queue_limit 10240        # 1MB * 10240 -> 10GB!
    
    flush_thread_count 16
  </buffer>
  
  auth_method private_key   # default
  email xxxxxxxxxxxx-xxxxxxxxxxxxxxxxxxxxxx@developer.gserviceaccount.com
  private_key_path /home/username/.keys/00000000000000000000000000000000-privatekey.p12
  # private_key_passphrase notasecret # default

  project yourproject_id
  dataset yourdataset_id
  tables  accesslog1,accesslog2,accesslog3
  
  field_integer time,status,bytes
  field_string  rhost,vhost,path,method,protocol,agent,referer
  field_float   requesttime
  field_boolean bot_access,loginsession
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
  @type bigquery

  method load

  <buffer>
  @type file
  path bigquery.*.buffer
  flush_interval 1800
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
  @type bigquery

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
  @type bigquery

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
  @type bigquery

  auth_method compute_engine

  project yourproject_id
  dataset yourdataset_id
  table   tablename
  
  field_integer time,status,bytes
  field_string  rhost,vhost,path,method,protocol,agent,referer
  field_float   requesttime
  field_boolean bot_access,loginsession
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

see. http://docs.fluentd.org/v0.14/articles/output-plugin-overview

For example, with the configuration below,
data is inserted into tables `accesslog_2014_08`, `accesslog_2014_09` and so on.

```apache
<match dummy>
  @type bigquery

  ...

  project yourproject_id
  dataset yourdataset_id
  table   accesslog_%Y_%m

  <buffer time>
    timekey 1d
  </buffer>
  ...
</match>
```

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
  @type bigquery

  ...
  table   accesslog$%Y%m%d

  <buffer time>
    timekey 1d
  </buffer>
  ...
</match>
```

But, Dynamic table creating doesn't support date partitioned table yet.

### Dynamic table creating

When `auto_create_table` is set to `true`, try to create the table using BigQuery API when insertion failed with code=404 "Not Found: Table ...".
Next retry of insertion is expected to be success.

NOTE: `auto_create_table` option cannot be used with `fetch_schema`. You should create the table on ahead to use `fetch_schema`.

```apache
<match dummy>
  @type bigquery

  ...

  auto_create_table true
  table accesslog_%Y_%m

  ...
</match>
```

### Table schema

There are three methods to describe the schema of the target table.

1. List fields in fluent.conf
2. Load a schema file in JSON.
3. Fetch a schema using BigQuery API

The examples above use the first method.  In this method,
you can also specify nested fields by prefixing their belonging record fields.

```apache
<match dummy>
  @type bigquery

  ...
  
  field_integer time,response.status,response.bytes
  field_string  request.vhost,request.path,request.method,request.protocol,request.agent,request.referer,remote.host,remote.ip,remote.user
  field_float   request.time
  field_boolean request.bot_access,request.loginsession
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
  @type bigquery

  ...
  
  schema_path /path/to/httpd.schema
  field_integer time
</match>
```
where /path/to/httpd.schema is a path to the JSON-encoded schema file which you used for creating the table on BigQuery.

The third method is to set `fetch_schema` to `true` to enable fetch a schema using BigQuery API.  In this case, your fluent.conf looks like:

```apache
<match dummy>
  @type bigquery

  ...
  
  fetch_schema true
  # fetch_schema_table other_table # if you want to fetch schema from other table
  field_integer time
</match>
```

__CAUTION: `fetch_schema` target cannot parse and replace placeholder.__

If you specify multiple tables in configuration file, plugin get all schema data from BigQuery and merge it.

NOTE: Since JSON does not define how to encode data of TIMESTAMP type,
you are still recommended to specify JSON types for TIMESTAMP fields as "time" field does in the example, if you use second or third method.

### Specifying insertId property

BigQuery uses `insertId` property to detect duplicate insertion requests (see [data consistency](https://cloud.google.com/bigquery/streaming-data-into-bigquery#dataconsistency) in Google BigQuery documents).
You can set `insert_id_field` option to specify the field to use as `insertId` property.

```apache
<match dummy>
  @type bigquery

  ...

  insert_id_field uuid
  field_string uuid
</match>
```

## TODO

* support optional data fields
* support NULLABLE/REQUIRED/REPEATED field options in field list style of configuration
* OAuth installed application credentials support
* Google API discovery expiration
* Error classes
* check row size limits

## Authors

* @tagomoris: First author, original version
* KAIZEN platform Inc.: Maintener, Since 2014.08.19
* @joker1007
