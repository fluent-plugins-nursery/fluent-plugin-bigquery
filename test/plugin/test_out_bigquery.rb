require 'helper'
require 'google/apis/bigquery_v2'
require 'google/api_client/auth/key_utils'
require 'googleauth'
require 'active_support/json'
require 'active_support/core_ext/hash'
require 'active_support/core_ext/object/json'


class BigQueryOutputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
  end

  CONFIG = %[
    table foo
    email foo@bar.example
    private_key_path /path/to/key
    project yourproject_id
    dataset yourdataset_id

    time_format %s
    time_field  time

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
  ]

  API_SCOPE = "https://www.googleapis.com/auth/bigquery"

  def create_driver(conf = CONFIG)
    Fluent::Test::TimeSlicedOutputTestDriver.new(Fluent::BigQueryOutput).configure(conf, true)
  end

  def stub_writer(driver)
    writer = driver.instance.writer
    stub(writer).get_auth { nil }
    writer
  end

  # ref. https://github.com/GoogleCloudPlatform/google-cloud-ruby/blob/ea2be47beb32615b2bf69f8a846a684f86c8328c/google-cloud-bigquery/test/google/cloud/bigquery/table_insert_test.rb#L141
  def failure_insert_errors(reason, error_count, insert_error_count)
    error = Google::Apis::BigqueryV2::ErrorProto.new(
      reason: reason
    )
    insert_error = Google::Apis::BigqueryV2::InsertAllTableDataResponse::InsertError.new(
      errors: [].fill(error, 0, error_count)
    )

    res = Google::Apis::BigqueryV2::InsertAllTableDataResponse.new(
      insert_errors: [].fill(insert_error, 0, insert_error_count)
    )
    return res
  end

  def test_configure_table
    driver = create_driver
    assert_equal driver.instance.table, 'foo'
    assert_nil driver.instance.tables

    driver = create_driver(CONFIG.sub(/\btable\s+.*$/,  'tables foo,bar'))
    assert_nil driver.instance.table
    assert_equal driver.instance.tables, 'foo,bar'

    assert_raise(Fluent::ConfigError, "'table' or 'tables' must be specified, and both are invalid") {
      create_driver(CONFIG + "tables foo,bar")
    }
  end

  def test_configure_auth_private_key
    key = stub!
    mock(Google::APIClient::KeyUtils).load_from_pkcs12('/path/to/key', 'notasecret') { key }
    authorization = Object.new
    stub(Signet::OAuth2::Client).new
    mock(Signet::OAuth2::Client).new(
      token_credential_uri: "https://accounts.google.com/o/oauth2/token",
      audience: "https://accounts.google.com/o/oauth2/token",
      scope: API_SCOPE,
      issuer: 'foo@bar.example',
      signing_key: key) { authorization }

    mock.proxy(Google::Apis::BigqueryV2::BigqueryService).new.with_any_args do |cl|
      mock(cl).__send__(:authorization=, authorization) {}
      cl
    end

    driver = create_driver
    mock.proxy(Fluent::BigQuery::Writer).new(duck_type(:info, :error, :warn), driver.instance.auth_method, is_a(Hash))
    driver.instance.writer
    assert driver.instance.writer.client.is_a?(Google::Apis::BigqueryV2::BigqueryService)
  end

  def test_configure_auth_compute_engine
    authorization = Object.new
    mock(Google::Auth::GCECredentials).new { authorization }

    mock.proxy(Google::Apis::BigqueryV2::BigqueryService).new.with_any_args do |cl|
      mock(cl).__send__(:authorization=, authorization) {}
      cl
    end

    driver = create_driver(%[
      table foo
      auth_method compute_engine
      project yourproject_id
      dataset yourdataset_id
      schema [
        {"name": "time", "type": "INTEGER"},
        {"name": "status", "type": "INTEGER"},
        {"name": "bytes", "type": "INTEGER"}
      ]
    ])
    mock.proxy(Fluent::BigQuery::Writer).new(duck_type(:info, :error, :warn), driver.instance.auth_method, is_a(Hash))
    driver.instance.writer
    assert driver.instance.writer.client.is_a?(Google::Apis::BigqueryV2::BigqueryService)
  end

  def test_configure_auth_json_key_as_file
    json_key_path = 'test/plugin/testdata/json_key.json'
    authorization = Object.new
    mock(Google::Auth::ServiceAccountCredentials).make_creds(json_key_io: File.open(json_key_path), scope: API_SCOPE) { authorization }

    mock.proxy(Google::Apis::BigqueryV2::BigqueryService).new.with_any_args do |cl|
      mock(cl).__send__(:authorization=, authorization) {}
      cl
    end

    driver = create_driver(%[
      table foo
      auth_method json_key
      json_key #{json_key_path}
      project yourproject_id
      dataset yourdataset_id
      schema [
        {"name": "time", "type": "INTEGER"},
        {"name": "status", "type": "INTEGER"},
        {"name": "bytes", "type": "INTEGER"}
      ]
    ])
    mock.proxy(Fluent::BigQuery::Writer).new(duck_type(:info, :error, :warn), driver.instance.auth_method, is_a(Hash))
    driver.instance.writer
    assert driver.instance.writer.client.is_a?(Google::Apis::BigqueryV2::BigqueryService)
  end

  def test_configure_auth_json_key_as_file_raise_permission_error
    json_key_path = 'test/plugin/testdata/json_key.json'
    json_key_path_dir = File.dirname(json_key_path)

    begin
      File.chmod(0000, json_key_path_dir)

      driver = create_driver(%[
        table foo
        auth_method json_key
        json_key #{json_key_path}
        project yourproject_id
        dataset yourdataset_id
        schema [
          {"name": "time", "type": "INTEGER"},
          {"name": "status", "type": "INTEGER"},
          {"name": "bytes", "type": "INTEGER"}
        ]
      ])
      assert_raises(Errno::EACCES) do
        driver.instance.writer.client
      end
    ensure
      File.chmod(0755, json_key_path_dir)
    end
  end

  def test_configure_auth_json_key_as_string
    json_key = '{"private_key": "X", "client_email": "' + 'x' * 255 + '@developer.gserviceaccount.com"}'
    json_key_io = StringIO.new(json_key)
    authorization = Object.new
    mock(Google::Auth::ServiceAccountCredentials).make_creds(json_key_io: satisfy {|arg| JSON.parse(arg.read) == JSON.parse(json_key_io.read) }, scope: API_SCOPE) { authorization }

    mock.proxy(Google::Apis::BigqueryV2::BigqueryService).new.with_any_args do |cl|
      mock(cl).__send__(:authorization=, authorization) {}
      cl
    end

    driver = create_driver(%[
      table foo
      auth_method json_key
      json_key #{json_key}
      project yourproject_id
      dataset yourdataset_id
      schema [
        {"name": "time", "type": "INTEGER"},
        {"name": "status", "type": "INTEGER"},
        {"name": "bytes", "type": "INTEGER"}
      ]
    ])
    mock.proxy(Fluent::BigQuery::Writer).new(duck_type(:info, :error, :warn), driver.instance.auth_method, is_a(Hash))
    driver.instance.writer
    assert driver.instance.writer.client.is_a?(Google::Apis::BigqueryV2::BigqueryService)
  end

  def test_configure_auth_application_default
    authorization = Object.new
    mock(Google::Auth).get_application_default([API_SCOPE]) { authorization }

    mock.proxy(Google::Apis::BigqueryV2::BigqueryService).new.with_any_args do |cl|
      mock(cl).__send__(:authorization=, authorization) {}
      cl
    end

    driver = create_driver(%[
      table foo
      auth_method application_default
      project yourproject_id
      dataset yourdataset_id
      schema [
        {"name": "time", "type": "INTEGER"},
        {"name": "status", "type": "INTEGER"},
        {"name": "bytes", "type": "INTEGER"}
      ]
    ])

    mock.proxy(Fluent::BigQuery::Writer).new(duck_type(:info, :error, :warn), driver.instance.auth_method, is_a(Hash))
    driver.instance.writer
    assert driver.instance.writer.client.is_a?(Google::Apis::BigqueryV2::BigqueryService)
  end

  def test_format_nested_time
    now = Time.now
    input = [
      now,
      {
        "metadata" => {
          "node" => "mynode.example",
        },
        "log" => "something",
      }
    ]
    expected = {
      "json" => {
        "metadata" => {
          "time" => now.strftime("%s").to_i,
          "node" => "mynode.example",
        },
        "log" => "something",
      }
    }

    driver = create_driver(<<-CONFIG)
      table foo
      email foo@bar.example
      private_key_path /path/to/key
      project yourproject_id
      dataset yourdataset_id

      time_format %s
      time_field  metadata.time

      schema [
        {"name": "metadata", "type": "RECORD", "fields": [
          {"name": "time", "type": "INTEGER"},
          {"name": "node", "type": "STRING"}
        ]},
        {"name": "log", "type": "STRING"}
      ]
    CONFIG

    driver.instance.start
    buf = driver.instance.format_stream("my.tag", [input])
    driver.instance.shutdown

    assert_equal expected, MessagePack.unpack(buf)
  end

  def test_format_with_schema
    now = Time.now
    input = [
      now,
      {
        "request" => {
          "vhost" => :bar,
          "path" => "/path/to/baz",
          "method" => "GET",
          "protocol" => "HTTP/0.9",
          "agent" => "libwww",
          "referer" => "http://referer.example",
          "time" => (now - 1).to_f,
          "bot_access" => true,
          "loginsession" => false,
        },
        "response" => {
          "status" => "1",
          "bytes" => 3.0,
        },
        "remote" => {
          "host" => "remote.example",
          "ip" =>  "192.0.2.1",
          "port" => 12345,
          "user" => "tagomoris",
        },
        "something-else" => "would be ignored",
        "yet-another" => {
          "foo" => "bar",
          "baz" => 1,
        },
      }
    ]
    expected = {
      "json" => {
        "time" => now.to_i,
        "request" => {
          "vhost" => "bar",
          "path" => "/path/to/baz",
          "method" => "GET",
          "protocol" => "HTTP/0.9",
          "agent" => "libwww",
          "referer" => "http://referer.example",
          "time" => (now - 1).to_f,
          "bot_access" => true,
          "loginsession" => false,
        },
        "remote" => {
          "host" => "remote.example",
          "ip" =>  "192.0.2.1",
          "port" => 12345,
          "user" => "tagomoris",
        },
        "response" => {
          "status" => 1,
          "bytes" => 3,
        },
        "something-else" => "would be ignored",
        "yet-another" => {
          "foo" => "bar",
          "baz" => 1,
        },
      }
    }

    driver = create_driver(<<-CONFIG)
      table foo
      email foo@bar.example
      private_key_path /path/to/key
      project yourproject_id
      dataset yourdataset_id

      time_format %s
      time_field  time

      schema_path #{File.join(File.dirname(__FILE__), "testdata", "apache.schema")}
      schema [{"name": "time", "type": "INTEGER"}]
    CONFIG
    driver.instance.start
    buf = driver.instance.format_stream("my.tag", [input])
    driver.instance.shutdown

    assert_equal expected, MessagePack.unpack(buf)
  end

  def test_format_repeated_field_with_schema
    now = Time.now
    input = [
      now,
      {
        "tty" => nil,
        "pwd" => "/home/yugui",
        "user" => "fluentd",
        "argv" => %w[ tail -f /var/log/fluentd/fluentd.log ]
      }
    ]
    expected = {
      "json" => {
        "time" => now.to_i,
        "pwd" => "/home/yugui",
        "user" => "fluentd",
        "argv" => %w[ tail -f /var/log/fluentd/fluentd.log ]
      }
    }

    driver = create_driver(<<-CONFIG)
      table foo
      email foo@bar.example
      private_key_path /path/to/key
      project yourproject_id
      dataset yourdataset_id

      time_format %s
      time_field  time

      schema_path #{File.join(File.dirname(__FILE__), "testdata", "sudo.schema")}
      schema [{"name": "time", "type": "INTEGER"}]
    CONFIG
    driver.instance.start
    buf = driver.instance.format_stream("my.tag", [input])
    driver.instance.shutdown

    assert_equal expected, MessagePack.unpack(buf)
  end

  def test_format_fetch_from_bigquery_api
    now = Time.now
    input = [
      now,
      {
        "tty" => nil,
        "pwd" => "/home/yugui",
        "user" => "fluentd",
        "argv" => %w[ tail -f /var/log/fluentd/fluentd.log ]
      }
    ]
    expected = {
      "json" => {
        "time" => now.to_i,
        "pwd" => "/home/yugui",
        "user" => "fluentd",
        "argv" => %w[ tail -f /var/log/fluentd/fluentd.log ]
      }
    }

    driver = create_driver(<<-CONFIG)
      table foo
      email foo@bar.example
      private_key_path /path/to/key
      project yourproject_id
      dataset yourdataset_id

      time_format %s
      time_field  time

      fetch_schema true
      schema [{"name": "time", "type": "INTEGER"}]
    CONFIG

    writer = stub_writer(driver)
    mock(writer).fetch_schema('yourproject_id', 'yourdataset_id', 'foo') do
      sudo_schema_response.deep_stringify_keys["schema"]["fields"]
    end
    driver.instance.start
    buf = driver.instance.format_stream("my.tag", [input])
    driver.instance.shutdown

    assert_equal expected, MessagePack.unpack(buf)

    fields = driver.instance.instance_eval{ @fields }
    assert fields["time"]
    assert_equal :integer, fields["time"].type  # DO NOT OVERWRITE
    assert_equal :nullable, fields["time"].mode # DO NOT OVERWRITE

    assert fields["tty"]
    assert_equal :string, fields["tty"].type
    assert_equal :nullable, fields["tty"].mode

    assert fields["pwd"]
    assert_equal :string, fields["pwd"].type
    assert_equal :required, fields["pwd"].mode

    assert fields["user"]
    assert_equal :string, fields["user"].type
    assert_equal :required, fields["user"].mode

    assert fields["argv"]
    assert_equal :string, fields["argv"].type
    assert_equal :repeated, fields["argv"].mode
  end

  def test_format_fetch_from_bigquery_api_with_generated_table_id
    now = Time.now
    input = [
      now,
      {
        "tty" => nil,
        "pwd" => "/home/yugui",
        "user" => "fluentd",
        "argv" => %w[ tail -f /var/log/fluentd/fluentd.log ]
      }
    ]
    expected = {
      "json" => {
        "time" => now.to_i,
        "pwd" => "/home/yugui",
        "user" => "fluentd",
        "argv" => %w[ tail -f /var/log/fluentd/fluentd.log ]
      }
    }

    driver = create_driver(<<-CONFIG)
      table foo_%Y_%m_%d
      email foo@bar.example
      private_key_path /path/to/key
      project yourproject_id
      dataset yourdataset_id

      time_format %s
      time_field  time

      fetch_schema true
      schema [{"name": "time", "type": "INTEGER"}]
    CONFIG

    writer = stub_writer(driver)
    mock(writer).fetch_schema('yourproject_id', 'yourdataset_id', now.strftime('foo_%Y_%m_%d')) do
      sudo_schema_response.deep_stringify_keys["schema"]["fields"]
    end
    driver.instance.start
    buf = driver.instance.format_stream("my.tag", [input])
    driver.instance.shutdown

    assert_equal expected, MessagePack.unpack(buf)

    fields = driver.instance.instance_eval{ @fields }
    assert fields["time"]
    assert_equal :integer, fields["time"].type  # DO NOT OVERWRITE
    assert_equal :nullable, fields["time"].mode # DO NOT OVERWRITE

    assert fields["tty"]
    assert_equal :string, fields["tty"].type
    assert_equal :nullable, fields["tty"].mode

    assert fields["pwd"]
    assert_equal :string, fields["pwd"].type
    assert_equal :required, fields["pwd"].mode

    assert fields["user"]
    assert_equal :string, fields["user"].type
    assert_equal :required, fields["user"].mode

    assert fields["argv"]
    assert_equal :string, fields["argv"].type
    assert_equal :repeated, fields["argv"].mode
  end

  def test_format_with_insert_id
    now = Time.now
    input = [
      now,
      {
        "uuid" => "9ABFF756-0267-4247-847F-0895B65F0938",
      }
    ]
    expected = {
      "insert_id" => "9ABFF756-0267-4247-847F-0895B65F0938",
      "json" => {
        "uuid" => "9ABFF756-0267-4247-847F-0895B65F0938",
      }
    }

    driver = create_driver(<<-CONFIG)
      table foo
      email foo@bar.example
      private_key_path /path/to/key
      project yourproject_id
      dataset yourdataset_id

      insert_id_field uuid
      schema [{"name": "uuid", "type": "STRING"}]
    CONFIG
    driver.instance.start
    buf = driver.instance.format_stream("my.tag", [input])
    driver.instance.shutdown

    assert_equal expected, MessagePack.unpack(buf)
  end

  def test_format_with_nested_insert_id
    now = Time.now
    input = [
      now,
      {
        "data" => {
          "uuid" => "809F6BA7-1C16-44CD-9816-4B20E2C7AA2A",
        },
      }
    ]
    expected = {
      "insert_id" => "809F6BA7-1C16-44CD-9816-4B20E2C7AA2A",
      "json" => {
        "data" => {
          "uuid" => "809F6BA7-1C16-44CD-9816-4B20E2C7AA2A",
        }
      }
    }

    driver = create_driver(<<-CONFIG)
      table foo
      email foo@bar.example
      private_key_path /path/to/key
      project yourproject_id
      dataset yourdataset_id

      insert_id_field data.uuid
      schema [{"name": "data", "type": "RECORD", "fields": [
        {"name": "uuid", "type": "STRING"}
      ]}]
    CONFIG
    driver.instance.start
    buf = driver.instance.format_stream("my.tag", [input])
    driver.instance.shutdown

    assert_equal expected, MessagePack.unpack(buf)
  end

  def test_format_for_load
    now = Time.now
    input = [
      now,
      {
        "uuid" => "9ABFF756-0267-4247-847F-0895B65F0938",
      }
    ]
    expected = MultiJson.dump({
      "uuid" => "9ABFF756-0267-4247-847F-0895B65F0938",
    }) + "\n"

    driver = create_driver(<<-CONFIG)
      method load
      table foo
      email foo@bar.example
      private_key_path /path/to/key
      project yourproject_id
      dataset yourdataset_id

      schema [{"name": "uuid", "type": "STRING"}]

      buffer_type memory
    CONFIG
    driver.instance.start
    buf = driver.instance.format_stream("my.tag", [input])
    driver.instance.shutdown

    assert_equal expected, buf
  end

  def test_replace_record_key
    now = Time.now
    input = [
      now,
      {
        "vhost" => :bar,
        "@referer" => "http://referer.example",
        "bot_access" => true,
        "login-session" => false
      }
    ]
    expected = {
      "json" => {
        "time" => now.to_i,
        "vhost" => "bar",
        "referer" => "http://referer.example",
        "bot_access" => true,
        "login_session" => false
      }
    }

    driver = create_driver(<<-CONFIG)
      table foo
      email foo@bar.example
      private_key_path /path/to/key
      project yourproject_id
      dataset yourdataset_id

      replace_record_key true
      replace_record_key_regexp1 - _

      time_format %s
      time_field time

      schema [
        {"name": "time", "type": "INTEGER"},
        {"name": "vhost", "type": "STRING"},
        {"name": "refere", "type": "STRING"},
        {"name": "bot_access", "type": "BOOLEAN"},
        {"name": "login_session", "type": "BOOLEAN"}
      ]
    CONFIG
    driver.instance.start
    buf = driver.instance.format_stream("my.tag", [input])
    driver.instance.shutdown

    assert_equal expected, MessagePack.unpack(buf)
  end

  def test_convert_hash_to_json
    now = Time.now
    input = [
      now,
      {
        "vhost" => :bar,
        "referer" => "http://referer.example",
        "bot_access" => true,
        "loginsession" => false,
        "remote" => {
          "host" => "remote.example",
          "ip" => "192.0.2.1",
          "port" => 12345,
          "user" => "tagomoris",
        }
      }
    ]
    expected = {
      "json" => {
        "time" => now.to_i,
        "vhost" => "bar",
        "referer" => "http://referer.example",
        "bot_access" => true,
        "loginsession" => false,
        "remote" => "{\"host\":\"remote.example\",\"ip\":\"192.0.2.1\",\"port\":12345,\"user\":\"tagomoris\"}"
      }
    }

    driver = create_driver(<<-CONFIG)
      table foo
      email foo@bar.example
      private_key_path /path/to/key
      project yourproject_id
      dataset yourdataset_id

      convert_hash_to_json true

      time_format %s
      time_field time

      schema [
        {"name": "time", "type": "INTEGER"},
        {"name": "vhost", "type": "STRING"},
        {"name": "refere", "type": "STRING"},
        {"name": "bot_access", "type": "BOOLEAN"},
        {"name": "loginsession", "type": "BOOLEAN"}
      ]
    CONFIG
    driver.instance.start
    buf = driver.instance.format_stream("my.tag", [input])
    driver.instance.shutdown

    assert_equal expected, MessagePack.unpack(buf)
  end

  def test_write
    entry = {json: {a: "b"}}, {json: {b: "c"}}
    driver = create_driver

    writer = stub_writer(driver)
    mock.proxy(writer).insert_rows('yourproject_id', 'yourdataset_id', 'foo', entry, template_suffix: nil)
    mock(writer.client).insert_all_table_data('yourproject_id', 'yourdataset_id', 'foo', {
      rows: entry,
      skip_invalid_rows: false,
      ignore_unknown_values: false
    }, {options: {timeout_sec: nil, open_timeout_sec: 60}}) do
      s = stub!
      s.insert_errors { nil }
      s
    end

    chunk = Fluent::MemoryBufferChunk.new("my.tag")
    entry.each do |e|
      chunk << e.to_msgpack
    end

    driver.instance.start
    driver.instance.write(chunk)
    driver.instance.shutdown
  end

  def test_write_with_retryable_error
    entry = {json: {a: "b"}}, {json: {b: "c"}}
    driver = create_driver(<<-CONFIG)
      table foo
      email foo@bar.example
      private_key_path /path/to/key
      project yourproject_id
      dataset yourdataset_id

      time_format %s
      time_field  time

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
      <secondary>
        type file
        path error
        utc
      </secondary>
    CONFIG

    writer = stub_writer(driver)
    mock(writer.client).insert_all_table_data('yourproject_id', 'yourdataset_id', 'foo', {
      rows: entry,
      skip_invalid_rows: false,
      ignore_unknown_values: false
    }, {options: {timeout_sec: nil, open_timeout_sec: 60}}) do
      ex = Google::Apis::ServerError.new("error", status_code: 500)
      raise ex
    end

    chunk = Fluent::MemoryBufferChunk.new("my.tag")
    entry.each do |e|
      chunk << e.to_msgpack
    end

    driver.instance.start
    assert_raise Fluent::BigQuery::RetryableError do
      driver.instance.write(chunk)
    end
    driver.instance.shutdown
  end

  def test_write_with_not_retryable_error
    entry = {json: {a: "b"}}, {json: {b: "c"}}
    driver = create_driver(<<-CONFIG)
      table foo
      email foo@bar.example
      private_key_path /path/to/key
      project yourproject_id
      dataset yourdataset_id

      time_format %s
      time_field  time

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
      <secondary>
        type file
        path error
        utc
      </secondary>
    CONFIG

    writer = stub_writer(driver)
    mock(writer.client).insert_all_table_data('yourproject_id', 'yourdataset_id', 'foo', {
      rows: entry,
      skip_invalid_rows: false,
      ignore_unknown_values: false
    }, {options: {timeout_sec: nil, open_timeout_sec: 60}}) do
      ex = Google::Apis::ServerError.new("error", status_code: 501)
      def ex.reason
        "invalid"
      end
      raise ex
    end

    mock(driver.instance).flush_secondary(is_a(Fluent::Output))

    chunk = Fluent::MemoryBufferChunk.new("my.tag")
    entry.each do |e|
      chunk << e.to_msgpack
    end

    driver.instance.start
    driver.instance.write(chunk)
    driver.instance.shutdown
  end

  def test_write_with_retryable_insert_errors
    data_input = [
      { "error_count" => 1,  "insert_error_count" => 1  },
      { "error_count" => 10, "insert_error_count" => 1  },
      { "error_count" => 10, "insert_error_count" => 10  },
    ]

    data_input.each do |d|
      entry = {json: {a: "b"}}, {json: {b: "c"}}
      allow_retry_insert_errors = true
      driver = create_driver(<<-CONFIG)
        table foo
        email foo@bar.example
        private_key_path /path/to/key
        project yourproject_id
        dataset yourdataset_id

        allow_retry_insert_errors #{allow_retry_insert_errors}

        time_format %s
        time_field  time

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
        <secondary>
          type file
          path error
          utc
        </secondary>
      CONFIG

      writer = stub_writer(driver)
      mock(writer.client).insert_all_table_data('yourproject_id', 'yourdataset_id', 'foo', {
        rows: entry,
        skip_invalid_rows: false,
        ignore_unknown_values: false
      }, {options: {timeout_sec: nil, open_timeout_sec: 60}}) do
        s = failure_insert_errors("timeout", d["error_count"], d["insert_error_count"])
        s
      end

      chunk = Fluent::MemoryBufferChunk.new("my.tag")
      entry.each do |e|
        chunk << e.to_msgpack
      end

      driver.instance.start
      assert_raise Fluent::BigQuery::RetryableError do
        driver.instance.write(chunk)
      end
      driver.instance.shutdown
    end
  end

  def test_write_with_not_retryable_insert_errors
    data_input = [
      { "allow_retry_insert_errors" => false, "reason" => "timeout" },
      { "allow_retry_insert_errors" => true,  "reason" => "stopped" },
    ]
    data_input.each do |d|
      entry = {json: {a: "b"}}, {json: {b: "c"}}
      driver = create_driver(<<-CONFIG)
        table foo
        email foo@bar.example
        private_key_path /path/to/key
        project yourproject_id
        dataset yourdataset_id

        allow_retry_insert_errors #{d["allow_retry_insert_errors"]}

        time_format %s
        time_field  time

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
        <secondary>
          type file
          path error
          utc
        </secondary>
      CONFIG

      writer = stub_writer(driver)
      mock(writer.client).insert_all_table_data('yourproject_id', 'yourdataset_id', 'foo', {
        rows: entry,
        skip_invalid_rows: false,
        ignore_unknown_values: false
      }, {options: {timeout_sec: nil, open_timeout_sec: 60}}) do
        s = failure_insert_errors(d["reason"], 1, 1)
        s
      end

      chunk = Fluent::MemoryBufferChunk.new("my.tag")
      entry.each do |e|
        chunk << e.to_msgpack
      end

      driver.instance.start
      driver.instance.write(chunk)
      driver.instance.shutdown
    end
  end

  def test_write_for_load
    schema_path = File.join(File.dirname(__FILE__), "testdata", "sudo.schema")
    entry = {a: "b"}, {b: "c"}
    driver = create_driver(<<-CONFIG)
      method load
      table foo
      email foo@bar.example
      private_key_path /path/to/key
      project yourproject_id
      dataset yourdataset_id

      time_format %s
      time_field  time

      schema_path #{schema_path}

      buffer_type memory
    CONFIG
    schema_fields = MultiJson.load(File.read(schema_path)).map(&:deep_symbolize_keys)

    writer = stub_writer(driver)
    chunk = Fluent::MemoryBufferChunk.new("my.tag")
    io = StringIO.new("hello")
    mock(driver.instance).create_upload_source(chunk).yields(io)
    mock(writer).wait_load_job(is_a(String), "yourproject_id", "yourdataset_id", "dummy_job_id", "foo") { nil }
    mock(writer.client).insert_job('yourproject_id', {
      configuration: {
        load: {
          destination_table: {
            project_id: 'yourproject_id',
            dataset_id: 'yourdataset_id',
            table_id: 'foo',
          },
          schema: {
            fields: schema_fields,
          },
          write_disposition: "WRITE_APPEND",
          source_format: "NEWLINE_DELIMITED_JSON",
          ignore_unknown_values: false,
          max_bad_records: 0,
        }
      }
    }, {upload_source: io, content_type: "application/octet-stream", options: {timeout_sec: nil, open_timeout_sec: 60}}) do
      s = stub!
      job_reference_stub = stub!
      s.job_reference { job_reference_stub }
      job_reference_stub.job_id { "dummy_job_id" }
      s
    end

    entry.each do |e|
      chunk << MultiJson.dump(e) + "\n"
    end

    driver.instance.start
    driver.instance.write(chunk)
    driver.instance.shutdown
  end

  def test_write_for_load_with_prevent_duplicate_load
    schema_path = File.join(File.dirname(__FILE__), "testdata", "sudo.schema")
    entry = {a: "b"}, {b: "c"}
    driver = create_driver(<<-CONFIG)
      method load
      table foo
      email foo@bar.example
      private_key_path /path/to/key
      project yourproject_id
      dataset yourdataset_id

      time_format %s
      time_field  time

      schema_path #{schema_path}
      prevent_duplicate_load true

      buffer_type memory
    CONFIG
    schema_fields = MultiJson.load(File.read(schema_path)).map(&:deep_symbolize_keys)

    chunk = Fluent::MemoryBufferChunk.new("my.tag")
    io = StringIO.new("hello")
    mock(driver.instance).create_upload_source(chunk).yields(io)
    writer = stub_writer(driver)
    mock(writer).wait_load_job(is_a(String), "yourproject_id", "yourdataset_id", "dummy_job_id", "foo") { nil }
    mock(writer.client).insert_job('yourproject_id', {
      configuration: {
        load: {
          destination_table: {
            project_id: 'yourproject_id',
            dataset_id: 'yourdataset_id',
            table_id: 'foo',
          },
          schema: {
            fields: schema_fields,
          },
          write_disposition: "WRITE_APPEND",
          source_format: "NEWLINE_DELIMITED_JSON",
          ignore_unknown_values: false,
          max_bad_records: 0,
        },
      },
      job_reference: {project_id: 'yourproject_id', job_id: satisfy { |x| x =~ /fluentd_job_.*/}} ,
    }, {upload_source: io, content_type: "application/octet-stream", options: {timeout_sec: nil, open_timeout_sec: 60}}) do
      s = stub!
      job_reference_stub = stub!
      s.job_reference { job_reference_stub }
      job_reference_stub.job_id { "dummy_job_id" }
      s
    end

    entry.each do |e|
      chunk << MultiJson.dump(e) + "\n"
    end

    driver.instance.start
    driver.instance.write(chunk)
    driver.instance.shutdown
  end

  def test_write_for_load_with_retryable_error
    schema_path = File.join(File.dirname(__FILE__), "testdata", "sudo.schema")
    entry = {a: "b"}, {b: "c"}
    driver = create_driver(<<-CONFIG)
      method load
      table foo
      email foo@bar.example
      private_key_path /path/to/key
      project yourproject_id
      dataset yourdataset_id

      time_format %s
      time_field  time

      schema_path #{schema_path}

      buffer_type memory
    CONFIG
    schema_fields = MultiJson.load(File.read(schema_path)).map(&:deep_symbolize_keys)

    chunk = Fluent::MemoryBufferChunk.new("my.tag")
    io = StringIO.new("hello")
    mock(driver.instance).create_upload_source(chunk).yields(io)
    writer = stub_writer(driver)
    mock(writer.client).insert_job('yourproject_id', {
      configuration: {
        load: {
          destination_table: {
            project_id: 'yourproject_id',
            dataset_id: 'yourdataset_id',
            table_id: 'foo',
          },
          schema: {
            fields: schema_fields,
          },
          write_disposition: "WRITE_APPEND",
          source_format: "NEWLINE_DELIMITED_JSON",
          ignore_unknown_values: false,
          max_bad_records: 0,
        }
      }
    }, {upload_source: io, content_type: "application/octet-stream", options: {timeout_sec: nil, open_timeout_sec: 60}}) do
      s = stub!
      job_reference_stub = stub!
      s.job_reference { job_reference_stub }
      job_reference_stub.job_id { "dummy_job_id" }
      s
    end

    mock(writer.client).get_job('yourproject_id', 'dummy_job_id') do
      s = stub!
      status_stub = stub!
      error_result = stub!

      s.status { status_stub }
      status_stub.state { "DONE" }
      status_stub.error_result { error_result }
      status_stub.errors { nil }
      error_result.message { "error" }
      error_result.reason { "backendError" }
      s
    end

    entry.each do |e|
      chunk << MultiJson.dump(e) + "\n"
    end

    driver.instance.start
    assert_raise Fluent::BigQuery::RetryableError do
      driver.instance.write(chunk)
    end
    driver.instance.shutdown
  end

  def test_write_for_load_with_not_retryable_error
    schema_path = File.join(File.dirname(__FILE__), "testdata", "sudo.schema")
    entry = {a: "b"}, {b: "c"}
    driver = create_driver(<<-CONFIG)
      method load
      table foo
      email foo@bar.example
      private_key_path /path/to/key
      project yourproject_id
      dataset yourdataset_id

      time_format %s
      time_field  time

      schema_path #{schema_path}

      buffer_type memory
      <secondary>
        type file
        path error
        utc
      </secondary>
    CONFIG
    schema_fields = MultiJson.load(File.read(schema_path)).map(&:deep_symbolize_keys)

    chunk = Fluent::MemoryBufferChunk.new("my.tag")
    io = StringIO.new("hello")
    mock(driver.instance).create_upload_source(chunk).yields(io)
    writer = stub_writer(driver)
    mock(writer.client).insert_job('yourproject_id', {
      configuration: {
        load: {
          destination_table: {
            project_id: 'yourproject_id',
            dataset_id: 'yourdataset_id',
            table_id: 'foo',
          },
          schema: {
            fields: schema_fields,
          },
          write_disposition: "WRITE_APPEND",
          source_format: "NEWLINE_DELIMITED_JSON",
          ignore_unknown_values: false,
          max_bad_records: 0,
        }
      }
    }, {upload_source: io, content_type: "application/octet-stream", options: {timeout_sec: nil, open_timeout_sec: 60}}) do
      s = stub!
      job_reference_stub = stub!
      s.job_reference { job_reference_stub }
      job_reference_stub.job_id { "dummy_job_id" }
      s
    end

    mock(writer.client).get_job('yourproject_id', 'dummy_job_id') do
      s = stub!
      status_stub = stub!
      error_result = stub!

      s.status { status_stub }
      status_stub.state { "DONE" }
      status_stub.error_result { error_result }
      status_stub.errors { nil }
      error_result.message { "error" }
      error_result.reason { "invalid" }
      s
    end

    mock(driver.instance).flush_secondary(is_a(Fluent::Output))

    entry.each do |e|
      chunk << MultiJson.dump(e) + "\n"
    end

    driver.instance.start
    driver.instance.write(chunk)
    driver.instance.shutdown
  end

  def test_write_with_row_based_table_id_formatting
    entry = [
      {json: {a: "b", created_at: Time.local(2014,8,20,9,0,0).to_i}},
      {json: {b: "c", created_at: Time.local(2014,8,21,9,0,0).to_i}}
    ]
    driver = create_driver(<<-CONFIG)
      table foo_%Y_%m_%d@created_at
      email foo@bar.example
      private_key_path /path/to/key
      project yourproject_id
      dataset yourdataset_id

      time_format %s
      time_field  time

      field_integer time,status,bytes
      field_string  vhost,path,method,protocol,agent,referer,remote.host,remote.ip,remote.user
      field_float   requesttime
      field_boolean bot_access,loginsession
    CONFIG

    writer = stub_writer(driver)
    mock(writer.client).insert_all_table_data('yourproject_id', 'yourdataset_id', 'foo_2014_08_20', {
      rows: [entry[0]],
      skip_invalid_rows: false,
      ignore_unknown_values: false
    }, {options: {timeout_sec: nil, open_timeout_sec: 60}}) { stub!.insert_errors { nil } }

    mock(writer.client).insert_all_table_data('yourproject_id', 'yourdataset_id', 'foo_2014_08_21', {
      rows: [entry[1]],
      skip_invalid_rows: false,
      ignore_unknown_values: false
    }, {options: {timeout_sec: nil, open_timeout_sec: 60}}) { stub!.insert_errors { nil } }

    chunk = Fluent::MemoryBufferChunk.new("my.tag")
    entry.each do |object|
      chunk << object.to_msgpack
    end

    driver.instance.start
    driver.instance.write(chunk)
    driver.instance.shutdown
  end

  def test_generate_table_id_without_row
    driver = create_driver
    table_id_format = 'foo_%Y_%m_%d'
    time = Time.local(2014, 8, 11, 21, 20, 56)
    table_id = driver.instance.generate_table_id(table_id_format, time, nil)
    assert_equal 'foo_2014_08_11', table_id
  end

  def test_generate_table_id_with_row
    driver = create_driver
    table_id_format = 'foo_%Y_%m_%d@created_at'
    time = Time.local(2014, 8, 11, 21, 20, 56)
    row = { json: { created_at: Time.local(2014,8,10,21,20,57).to_i } }
    table_id = driver.instance.generate_table_id(table_id_format, time, row)
    assert_equal 'foo_2014_08_10', table_id
  end

  def test_generate_table_id_with_row_nested_attribute
    driver = create_driver
    table_id_format = 'foo_%Y_%m_%d@foo.bar.created_at'
    time = Time.local(2014, 8, 11, 21, 20, 56)
    row = { json: { foo: { bar: { created_at: Time.local(2014,8,10,21,20,57).to_i } } } }
    table_id = driver.instance.generate_table_id(table_id_format, time, row)
    assert_equal 'foo_2014_08_10', table_id
  end

  def test_generate_table_id_with_time_sliced_format
    driver = create_driver
    table_id_format = 'foo_%{time_slice}'
    current_time = Time.now
    time = Time.local(2014, 8, 11, 21, 20, 56)
    row = { "json" => { "foo" => "bar", "time" => time.to_i } }
    chunk = Object.new
    mock(chunk).key { time.strftime("%Y%m%d") }
    table_id = driver.instance.generate_table_id(table_id_format, current_time, row, chunk)
    assert_equal 'foo_20140811', table_id
  end

  def test_generate_table_id_with_attribute_replacement
    driver = create_driver
    table_id_format = 'foo_%Y_%m_%d_${baz}'
    current_time = Time.now
    time = Time.local(2014, 8, 11, 21, 20, 56)
    [
      [ { baz: 1234 },         'foo_2014_08_11_1234' ],
      [ { baz: 'piyo' },       'foo_2014_08_11_piyo' ],
      [ { baz: true },         'foo_2014_08_11_true' ],
      [ { baz: nil },          'foo_2014_08_11_' ],
      [ { baz: '' },           'foo_2014_08_11_' ],
      [ { baz: "_X-Y.Z !\n" }, 'foo_2014_08_11__XYZ' ],
      [ { baz: { xyz: 1 } },   'foo_2014_08_11_xyz1' ],
    ].each do |attrs, expected|
      row = { json: { created_at: Time.local(2014,8,10,21,20,57).to_i }.merge(attrs) }
      table_id = driver.instance.generate_table_id(table_id_format, time, row)
      assert_equal expected, table_id
    end
  end

  def test_auto_create_table_by_bigquery_api
    now = Time.now
    message = {
      "json" => {
        "time" => now.to_i,
        "request" => {
          "vhost" => "bar",
          "path" => "/path/to/baz",
          "method" => "GET",
          "protocol" => "HTTP/1.0",
          "agent" => "libwww",
          "referer" => "http://referer.example",
          "time" => (now - 1).to_f,
          "bot_access" => true,
          "loginsession" => false,
        },
        "remote" => {
          "host" => "remote.example",
          "ip" =>  "192.168.1.1",
          "user" => "nagachika",
        },
        "response" => {
          "status" => 200,
          "bytes" => 72,
        },
      }
    }.deep_symbolize_keys

    driver = create_driver(<<-CONFIG)
      table foo
      email foo@bar.example
      private_key_path /path/to/key
      project yourproject_id
      dataset yourdataset_id

      time_format %s
      time_field  time

      auto_create_table true
      schema_path #{File.join(File.dirname(__FILE__), "testdata", "apache.schema")}
    CONFIG
    writer = stub_writer(driver)
    mock(writer).insert_rows('yourproject_id', 'yourdataset_id', 'foo', [message], template_suffix: nil) { raise Fluent::BigQuery::RetryableError.new(nil, Google::Apis::ServerError.new("Not found: Table yourproject_id:yourdataset_id.foo", status_code: 404, body: "Not found: Table yourproject_id:yourdataset_id.foo")) }
    mock(writer).create_table('yourproject_id', 'yourdataset_id', 'foo', driver.instance.instance_variable_get(:@fields))

    chunk = Fluent::MemoryBufferChunk.new("my.tag")
    chunk << message.to_msgpack

    driver.instance.start

    assert_raise(RuntimeError) {
      driver.instance.write(chunk)
    }
    driver.instance.shutdown
  end

  def test_auto_create_partitioned_table_by_bigquery_api
    now = Time.now
    message = {
      "json" => {
        "time" => now.to_i,
        "request" => {
          "vhost" => "bar",
          "path" => "/path/to/baz",
          "method" => "GET",
          "protocol" => "HTTP/1.0",
          "agent" => "libwww",
          "referer" => "http://referer.example",
          "time" => (now - 1).to_f,
          "bot_access" => true,
          "loginsession" => false,
        },
        "remote" => {
          "host" => "remote.example",
          "ip" =>  "192.168.1.1",
          "user" => "nagachika",
        },
        "response" => {
          "status" => 200,
          "bytes" => 72,
        },
      }
    }.deep_symbolize_keys

    driver = create_driver(<<-CONFIG)
      table foo
      email foo@bar.example
      private_key_path /path/to/key
      project yourproject_id
      dataset yourdataset_id

      time_format %s
      time_field  time

      auto_create_table true
      schema_path #{File.join(File.dirname(__FILE__), "testdata", "apache.schema")}

      time_partitioning_type day
      time_partitioning_expiration 1h
    CONFIG
    writer = stub_writer(driver)
    mock(writer).insert_rows('yourproject_id', 'yourdataset_id', 'foo', [message], template_suffix: nil) { raise Fluent::BigQuery::RetryableError.new(nil, Google::Apis::ServerError.new("Not found: Table yourproject_id:yourdataset_id.foo", status_code: 404, body: "Not found: Table yourproject_id:yourdataset_id.foo")) }
    mock(writer).create_table('yourproject_id', 'yourdataset_id', 'foo', driver.instance.instance_variable_get(:@fields))

    chunk = Fluent::MemoryBufferChunk.new("my.tag")
    chunk << message.to_msgpack

    driver.instance.start

    assert_raise(RuntimeError) {
      driver.instance.write(chunk)
    }
    driver.instance.shutdown
  end

  private

  def sudo_schema_response
    {
      schema: {
        fields: [
          {
            name: "time",
            type: "TIMESTAMP",
            mode: "REQUIRED"
          },
          {
            name: "tty",
            type: "STRING",
            mode: "NULLABLE"
          },
          {
            name: "pwd",
            type: "STRING",
            mode: "REQUIRED"
          },
          {
            name: "user",
            type: "STRING",
            mode: "REQUIRED"
          },
          {
            name: "argv",
            type: "STRING",
            mode: "REPEATED"
          }
        ]
      }
    }
  end
end
