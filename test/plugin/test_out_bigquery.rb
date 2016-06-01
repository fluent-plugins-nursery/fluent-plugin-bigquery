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

    field_integer time,status,bytes
    field_string  vhost,path,method,protocol,agent,referer,remote.host,remote.ip,remote.user
    field_float   requesttime
    field_boolean bot_access,loginsession
  ]

  API_SCOPE = "https://www.googleapis.com/auth/bigquery"

  def create_driver(conf = CONFIG)
    Fluent::Test::TimeSlicedOutputTestDriver.new(Fluent::BigQueryOutput).configure(conf)
  end

  def stub_client(driver)
    stub(client = Object.new) do |expect|
      yield expect if defined?(yield)
    end
    stub(driver.instance).client { client }
    client
  end

  def mock_client(driver)
    mock(client = Object.new) do |expect|
      yield expect
    end
    stub(driver.instance).client { client }
    client
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

    mock.proxy(Google::Apis::BigqueryV2::BigqueryService).new.with_any_args {
      mock!.__send__(:authorization=, authorization) {}
    }

    driver = create_driver(CONFIG)
    driver.instance.client()
  end

  def test_configure_auth_compute_engine
    authorization = Object.new
    mock(Google::Auth::GCECredentials).new { authorization }

    mock.proxy(Google::Apis::BigqueryV2::BigqueryService).new.with_any_args {
      mock!.__send__(:authorization=, authorization) {}
    }

    driver = create_driver(%[
      table foo
      auth_method compute_engine
      project yourproject_id
      dataset yourdataset_id
      field_integer time,status,bytes
    ])
    driver.instance.client()
  end

  def test_configure_auth_json_key_as_file
    json_key_path = 'test/plugin/testdata/json_key.json'
    authorization = Object.new
    mock(Google::Auth::ServiceAccountCredentials).make_creds(json_key_io: File.open(json_key_path), scope: API_SCOPE) { authorization }

    mock.proxy(Google::Apis::BigqueryV2::BigqueryService).new.with_any_args {
      mock!.__send__(:authorization=, authorization) {}
    }

    driver = create_driver(%[
      table foo
      auth_method json_key
      json_key #{json_key_path}
      project yourproject_id
      dataset yourdataset_id
      field_integer time,status,bytes
    ])
    driver.instance.client()
  end

  def test_configure_auth_json_key_as_string
    json_key = '{"private_key": "X", "client_email": "xxx@developer.gserviceaccount.com"}'
    json_key_io = StringIO.new(json_key)
    mock(StringIO).new(json_key) { json_key_io }
    authorization = Object.new
    mock(Google::Auth::ServiceAccountCredentials).make_creds(json_key_io: json_key_io, scope: API_SCOPE) { authorization }

    mock.proxy(Google::Apis::BigqueryV2::BigqueryService).new.with_any_args {
      mock!.__send__(:authorization=, authorization) {}
    }

    driver = create_driver(%[
      table foo
      auth_method json_key
      json_key #{json_key}
      project yourproject_id
      dataset yourdataset_id
      field_integer time,status,bytes
    ])
    driver.instance.client()
  end

  def test_configure_auth_application_default
    authorization = Object.new
    mock(Google::Auth).get_application_default([API_SCOPE]) { authorization }

    mock.proxy(Google::Apis::BigqueryV2::BigqueryService).new.with_any_args {
      mock!.__send__(:authorization=, authorization) {}
    }

    driver = create_driver(%[
      table foo
      auth_method application_default
      project yourproject_id
      dataset yourdataset_id
      field_integer time,status,bytes
    ])
    driver.instance.client()
  end

  def test_configure_fieldname_stripped
    driver = create_driver(%[
      table foo
      email foo@bar.example
      private_key_path /path/to/key
      project yourproject_id
      dataset yourdataset_id

      time_format %s
      time_field  time

      field_integer time  , status , bytes
      field_string  _log_name, vhost, path, method, protocol, agent, referer, remote.host, remote.ip, remote.user
      field_float   requesttime
      field_boolean bot_access , loginsession
    ])
    fields = driver.instance.instance_eval{ @fields }

    assert (not fields['time  ']), "tailing spaces must be stripped"
    assert fields['time']
    assert fields['status']
    assert fields['bytes']
    assert fields['_log_name']
    assert fields['vhost']
    assert fields['protocol']
    assert fields['agent']
    assert fields['referer']
    assert fields['remote']['host']
    assert fields['remote']['ip']
    assert fields['remote']['user']
    assert fields['requesttime']
    assert fields['bot_access']
    assert fields['loginsession']
  end

  def test_configure_invalid_fieldname
    base = %[
      table foo
      email foo@bar.example
      private_key_path /path/to/key
      project yourproject_id
      dataset yourdataset_id

      time_format %s
      time_field  time
    ]

    assert_raises(Fluent::ConfigError) do
      create_driver(base + "field_integer time field\n")
    end
    assert_raises(Fluent::ConfigError) do
      create_driver(base + "field_string my name\n")
    end
    assert_raises(Fluent::ConfigError) do
      create_driver(base + "field_string remote.host name\n")
    end
    assert_raises(Fluent::ConfigError) do
      create_driver(base + "field_string 1column\n")
    end
    assert_raises(Fluent::ConfigError) do
      create_driver(base + "field_string #{'tenstrings' * 12 + '123456789'}\n")
    end
    assert_raises(Fluent::ConfigError) do
      create_driver(base + "field_float request time\n")
    end
    assert_raises(Fluent::ConfigError) do
      create_driver(base + "field_boolean login session\n")
    end
  end

  def test_format_stream
    now = Time.now
    input = [
      now,
      {
        "status" => "1",
        "bytes" => 3.0,
        "vhost" => :bar,
        "path" => "/path/to/baz",
        "method" => "GET",
        "protocol" => "HTTP/0.9",
        "agent" => "libwww",
        "referer" => "http://referer.example",
        "requesttime" => (now - 1).to_f.to_s,
        "bot_access" => true,
        "loginsession" => false,
        "something-else" => "would be ignored",
        "yet-another" => {
          "foo" => "bar",
          "baz" => 1,
        },
        "remote" => {
          "host" => "remote.example",
          "ip" =>  "192.0.2.1",
          "port" => 12345,
          "user" => "tagomoris",
        }
      }
    ]
    expected = {
      "json" => {
        "time" => now.to_i,
        "status" => 1,
        "bytes" => 3,
        "vhost" => "bar",
        "path" => "/path/to/baz",
        "method" => "GET",
        "protocol" => "HTTP/0.9",
        "agent" => "libwww",
        "referer" => "http://referer.example",
        "requesttime" => (now - 1).to_f.to_s.to_f,
        "bot_access" => true,
        "loginsession" => false,
        "something-else" => "would be ignored",
        "yet-another" => {
          "foo" => "bar",
          "baz" => 1,
        },
        "remote" => {
          "host" => "remote.example",
          "ip" =>  "192.0.2.1",
          "port" => 12345,
          "user" => "tagomoris",
        }
      }
    }

    driver = create_driver(CONFIG)
    driver.instance.start
    buf = driver.instance.format_stream("my.tag", [input])
    driver.instance.shutdown

    assert_equal expected, MessagePack.unpack(buf)
  end

  [
    # <time_format>, <time field type>, <time expectation generator>, <assertion>
    [
      "%s.%6N", "field_float",
      lambda{|t| t.strftime("%s.%6N").to_f },
      lambda{|recv, expected, actual|
        recv.assert_in_delta(expected, actual, Float::EPSILON / 10**3)
      }
    ],
    [
      "%Y-%m-%dT%H:%M:%SZ", "field_string",
      lambda{|t| t.iso8601 },
      :assert_equal.to_proc
    ],
    [
      "%a, %d %b %Y %H:%M:%S GMT", "field_string",
      lambda{|t| t.httpdate },
      :assert_equal.to_proc
    ],
  ].each do |format, type, expect_time, assert|
    define_method("test_time_formats_#{format}") do
      now = Time.now.utc
      input = [ now, {} ]
      expected = { "json" => { "time" => expect_time[now], } }

      driver = create_driver(<<-CONFIG)
        table foo
        email foo@bar.example
        private_key_path /path/to/key
        project yourproject_id
        dataset yourdataset_id

        time_format #{format}
        time_field  time
        #{type}     time
      CONFIG
      stub_client(driver)

      driver.instance.start
      buf = driver.instance.format_stream("my.tag", [input])
      driver.instance.shutdown

      assert[self, expected["json"]["time"], MessagePack.unpack(buf)["json"]["time"]]
    end
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

      field_integer metadata.time
      field_string  metadata.node,log
    CONFIG
    stub_client(driver)
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
      field_integer time
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
      field_integer time
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
      field_integer time
    CONFIG
    mock_client(driver) do |expect|
      expect.get_table('yourproject_id', 'yourdataset_id', 'foo') {
        s = stub!
        schema_stub = stub!
        fields_stub = stub!
        s.schema { schema_stub }
        schema_stub.fields { fields_stub }
        fields_stub.as_json { sudo_schema_response.deep_stringify_keys["schema"]["fields"] }
        s
      }
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
      field_integer time
    CONFIG
    mock_client(driver) do |expect|
      expect.get_table('yourproject_id', 'yourdataset_id', now.strftime('foo_%Y_%m_%d')) {
        s = stub!
        schema_stub = stub!
        fields_stub = stub!
        s.schema { schema_stub }
        schema_stub.fields { fields_stub }
        fields_stub.as_json { sudo_schema_response.deep_stringify_keys["schema"]["fields"] }
        s
      }
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
      field_string uuid
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
      field_string data.uuid
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

      field_string uuid
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

      field_integer time
      field_string vhost, referer
      field_boolean bot_access, login_session
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

      field_integer time
      field_string vhost, referer, remote
      field_boolean bot_access, loginsession
    CONFIG
    driver.instance.start
    buf = driver.instance.format_stream("my.tag", [input])
    driver.instance.shutdown

    assert_equal expected, MessagePack.unpack(buf)
  end

  def test_write
    entry = {json: {a: "b"}}, {json: {b: "c"}}
    driver = create_driver(CONFIG)
    mock_client(driver) do |expect|
      expect.insert_all_table_data('yourproject_id', 'yourdataset_id', 'foo', {
        rows: entry,
        skip_invalid_rows: false,
        ignore_unknown_values: false
      }, {options: {timeout_sec: nil, open_timeout_sec: 60}}) {
        s = stub!
        s.insert_errors { nil }
        s
      }
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

      field_integer time,status,bytes
      field_string  vhost,path,method,protocol,agent,referer,remote.host,remote.ip,remote.user
      field_float   requesttime
      field_boolean bot_access,loginsession
      <secondary>
        type file
        path error
        utc
      </secondary>
    CONFIG
    mock_client(driver) do |expect|
      expect.insert_all_table_data('yourproject_id', 'yourdataset_id', 'foo', {
        rows: entry,
        skip_invalid_rows: false,
        ignore_unknown_values: false
      }, {options: {timeout_sec: nil, open_timeout_sec: 60}}) {
        ex = Google::Apis::ServerError.new("error")
        def ex.reason
          "backendError"
        end
        raise ex
      }
    end

    chunk = Fluent::MemoryBufferChunk.new("my.tag")
    entry.each do |e|
      chunk << e.to_msgpack
    end

    driver.instance.start
    assert_raise RuntimeError do
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

      field_integer time,status,bytes
      field_string  vhost,path,method,protocol,agent,referer,remote.host,remote.ip,remote.user
      field_float   requesttime
      field_boolean bot_access,loginsession
      <secondary>
        type file
        path error
        utc
      </secondary>
    CONFIG
    mock_client(driver) do |expect|
      expect.insert_all_table_data('yourproject_id', 'yourdataset_id', 'foo', {
        rows: entry,
        skip_invalid_rows: false,
        ignore_unknown_values: false
      }, {options: {timeout_sec: nil, open_timeout_sec: 60}}) {
        ex = Google::Apis::ServerError.new("error")
        def ex.reason
          "invalid"
        end
        raise ex
      }
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
      field_integer time
    CONFIG
    schema_fields = MultiJson.load(File.read(schema_path)).map(&:deep_symbolize_keys).tap do |h|
      h[0][:type] = "INTEGER"
      h[0][:mode] = "NULLABLE"
    end

    chunk = Fluent::MemoryBufferChunk.new("my.tag")
    io = StringIO.new("hello")
    mock(driver.instance).create_upload_source(chunk).yields(io)
    mock(driver.instance).wait_load("dummy_job_id", "foo") { true }
    mock_client(driver) do |expect|
      expect.insert_job('yourproject_id', {
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
      }, {upload_source: io, content_type: "application/octet-stream", options: {timeout_sec: nil, open_timeout_sec: 60}}) {
        s = stub!
        job_reference_stub = stub!
        s.job_reference { job_reference_stub }
        job_reference_stub.job_id { "dummy_job_id" }
        s
      }
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
      field_integer time
      prevent_duplicate_load true
    CONFIG
    schema_fields = MultiJson.load(File.read(schema_path)).map(&:deep_symbolize_keys).tap do |h|
      h[0][:type] = "INTEGER"
      h[0][:mode] = "NULLABLE"
    end

    chunk = Fluent::MemoryBufferChunk.new("my.tag")
    io = StringIO.new("hello")
    mock(driver.instance).create_upload_source(chunk).yields(io)
    mock(driver.instance).wait_load("dummy_job_id", "foo") { true }
    mock_client(driver) do |expect|
      expect.insert_job('yourproject_id', {
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
      }, {upload_source: io, content_type: "application/octet-stream", options: {timeout_sec: nil, open_timeout_sec: 60}}) {
        s = stub!
        job_reference_stub = stub!
        s.job_reference { job_reference_stub }
        job_reference_stub.job_id { "dummy_job_id" }
        s
      }
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
      field_integer time
    CONFIG
    schema_fields = MultiJson.load(File.read(schema_path)).map(&:deep_symbolize_keys).tap do |h|
      h[0][:type] = "INTEGER"
      h[0][:mode] = "NULLABLE"
    end

    chunk = Fluent::MemoryBufferChunk.new("my.tag")
    io = StringIO.new("hello")
    mock(driver.instance).create_upload_source(chunk).yields(io)
    mock_client(driver) do |expect|
      expect.insert_job('yourproject_id', {
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
      }, {upload_source: io, content_type: "application/octet-stream", options: {timeout_sec: nil, open_timeout_sec: 60}}) {
        s = stub!
        job_reference_stub = stub!
        s.job_reference { job_reference_stub }
        job_reference_stub.job_id { "dummy_job_id" }
        s
      }

      expect.get_job('yourproject_id', 'dummy_job_id') {
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
      }
    end

    entry.each do |e|
      chunk << MultiJson.dump(e) + "\n"
    end

    driver.instance.start
    assert_raise RuntimeError do
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
      field_integer time
      <secondary>
        type file
        path error
        utc
      </secondary>
    CONFIG
    schema_fields = MultiJson.load(File.read(schema_path)).map(&:deep_symbolize_keys).tap do |h|
      h[0][:type] = "INTEGER"
      h[0][:mode] = "NULLABLE"
    end

    chunk = Fluent::MemoryBufferChunk.new("my.tag")
    io = StringIO.new("hello")
    mock(driver.instance).create_upload_source(chunk).yields(io)
    mock_client(driver) do |expect|
      expect.insert_job('yourproject_id', {
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
      }, {upload_source: io, content_type: "application/octet-stream", options: {timeout_sec: nil, open_timeout_sec: 60}}) {
        s = stub!
        job_reference_stub = stub!
        s.job_reference { job_reference_stub }
        job_reference_stub.job_id { "dummy_job_id" }
        s
      }

      expect.get_job('yourproject_id', 'dummy_job_id') {
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
      }
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
    mock_client(driver) do |expect|
      expect.insert_all_table_data('yourproject_id', 'yourdataset_id', 'foo_2014_08_20', {
        rows: [entry[0]],
        skip_invalid_rows: false,
        ignore_unknown_values: false
      }, {options: {timeout_sec: nil, open_timeout_sec: 60}}) { stub!.insert_errors { nil } }

      expect.insert_all_table_data('yourproject_id', 'yourdataset_id', 'foo_2014_08_21', {
        rows: [entry[1]],
        skip_invalid_rows: false,
        ignore_unknown_values: false
      }, {options: {timeout_sec: nil, open_timeout_sec: 60}}) { stub!.insert_errors { nil } }
    end

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
    mock_client(driver) do |expect|
      expect.insert_all_table_data('yourproject_id', 'yourdataset_id', 'foo', {
        rows: [message],
        skip_invalid_rows: false,
        ignore_unknown_values: false
      }, {options: {timeout_sec: nil, open_timeout_sec: 60}}) {
        raise Google::Apis::ServerError.new("Not found: Table yourproject_id:yourdataset_id.foo", status_code: 404, body: "Not found: Table yourproject_id:yourdataset_id.foo")
      }
      expect.insert_table('yourproject_id', 'yourdataset_id', {
        table_reference: {
          table_id: 'foo',
        },
        schema: {
          fields: JSON.parse(File.read(File.join(File.dirname(__FILE__), "testdata", "apache.schema"))).map(&:deep_symbolize_keys),
        }
      }, {}) {
        stub!
      }
    end
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
