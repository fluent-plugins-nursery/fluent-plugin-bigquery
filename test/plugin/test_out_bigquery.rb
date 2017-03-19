require 'helper'

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

    <inject>
    time_format %s
    time_key  time
    </inject>

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
    Fluent::Test::Driver::Output.new(Fluent::Plugin::BigQueryOutput).configure(conf)
  end

  def stub_writer(driver)
    writer = driver.instance.writer
    stub(writer).get_auth { nil }
    writer
  end

  def test_configure_table
    driver = create_driver
    assert_equal driver.instance.table, 'foo'
    assert_nil driver.instance.tables

    driver = create_driver(CONFIG.sub(/\btable\s+.*$/,  'tables foo,bar'))
    assert_nil driver.instance.table
    assert_equal driver.instance.tables, ['foo' ,'bar']

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

  def test_format
    now = Fluent::EventTime.new(Time.now.to_i)
    input = {
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
    expected = {
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

    driver = create_driver(CONFIG)
    buf = nil
    driver.run { buf = driver.instance.format("my.tag", now, input) }

    assert_equal expected, MultiJson.load(buf)
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
      "%Y-%m-%dT%H:%M:%S%:z", "field_string",
      lambda{|t| t.iso8601 },
      :assert_equal.to_proc
    ],
  ].each do |format, type, expect_time, assert|
    define_method("test_time_formats_#{format}") do
      now = Fluent::Engine.now
      input = {}
      expected = { "time" => expect_time[Time.at(now.to_r)] }

      driver = create_driver(<<-CONFIG)
        table foo
        email foo@bar.example
        private_key_path /path/to/key
        project yourproject_id
        dataset yourdataset_id

        <inject>
        time_format #{format}
        time_type string
        time_key  time
        </inject>
        #{type}     time

        schema [
          {"name": "metadata", "type": "RECORD", "fields": [
            {"name": "time", "type": "INTEGER"},
            {"name": "node", "type": "STRING"}
          ]},
          {"name": "log", "type": "STRING"}
        ]
      CONFIG

      buf = nil
      driver.run { buf = driver.instance.format("my.tag", now, input) }

      assert[self, expected["time"], MultiJson.load(buf)["time"]]
    end
  end

  def test_format_with_schema
    now = Fluent::EventTime.new(Time.now.to_i)
    input = {
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
    expected = {
      "time" => now.to_f,
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

    driver = create_driver(<<-CONFIG)
      table foo
      email foo@bar.example
      private_key_path /path/to/key
      project yourproject_id
      dataset yourdataset_id

      <inject>
      time_format %s
      time_key  time
      </inject>

      schema_path #{File.join(File.dirname(__FILE__), "testdata", "apache.schema")}
      schema [{"name": "time", "type": "INTEGER"}]
    CONFIG

    buf = nil
    driver.run { buf = driver.instance.format("my.tag", now, input) }

    assert_equal expected, MultiJson.load(buf)
  end

  def test_format_repeated_field_with_schema
    now = Fluent::EventTime.new(Time.now.to_i)
    input = {
      "tty" => nil,
      "pwd" => "/home/yugui",
      "user" => "fluentd",
      "argv" => %w[ tail -f /var/log/fluentd/fluentd.log ]
    }
    expected = {
      "time" => now.to_f,
      "pwd" => "/home/yugui",
      "user" => "fluentd",
      "argv" => %w[ tail -f /var/log/fluentd/fluentd.log ]
    }

    driver = create_driver(<<-CONFIG)
      table foo
      email foo@bar.example
      private_key_path /path/to/key
      project yourproject_id
      dataset yourdataset_id

      <inject>
      time_format %s
      time_key  time
      </inject>

      schema_path #{File.join(File.dirname(__FILE__), "testdata", "sudo.schema")}
      schema [{"name": "time", "type": "INTEGER"}]
    CONFIG

    buf = nil
    driver.run { buf = driver.instance.format("my.tag", now, input) }

    assert_equal expected, MultiJson.load(buf)
  end

  def test_format_fetch_from_bigquery_api
    now = Fluent::EventTime.new(Time.now.to_i)
    input = {
      "tty" => nil,
      "pwd" => "/home/yugui",
      "user" => "fluentd",
      "argv" => %w[ tail -f /var/log/fluentd/fluentd.log ]
    }
    expected = {
      "time" => now.to_i,
      "pwd" => "/home/yugui",
      "user" => "fluentd",
      "argv" => %w[ tail -f /var/log/fluentd/fluentd.log ]
    }

    driver = create_driver(<<-CONFIG)
      table foo
      email foo@bar.example
      private_key_path /path/to/key
      project yourproject_id
      dataset yourdataset_id

      <inject>
      time_format %s
      time_key  time
      </inject>

      fetch_schema true
      schema [{"name": "time", "type": "INTEGER"}]
    CONFIG

    writer = stub_writer(driver)
    mock(writer).fetch_schema('yourproject_id', 'yourdataset_id', 'foo') do
      sudo_schema_response.deep_stringify_keys["schema"]["fields"]
    end

    buf = nil
    driver.run { buf = driver.instance.format("my.tag", now, input) }

    assert_equal expected, MultiJson.load(buf)

    table_schema = driver.instance.instance_eval{ get_schema('yourproject_id', 'yourdataset_id', 'foo') }
    assert table_schema["time"]
    assert_equal :timestamp, table_schema["time"].type
    assert_equal :required, table_schema["time"].mode

    assert table_schema["tty"]
    assert_equal :string, table_schema["tty"].type
    assert_equal :nullable, table_schema["tty"].mode

    assert table_schema["pwd"]
    assert_equal :string, table_schema["pwd"].type
    assert_equal :required, table_schema["pwd"].mode

    assert table_schema["user"]
    assert_equal :string, table_schema["user"].type
    assert_equal :required, table_schema["user"].mode

    assert table_schema["argv"]
    assert_equal :string, table_schema["argv"].type
    assert_equal :repeated, table_schema["argv"].mode
  end

  def test_format_fetch_from_bigquery_api_with_fetch_schema_table
    now = Fluent::EventTime.new(Time.now.to_i)
    input = {
      "tty" => nil,
      "pwd" => "/home/yugui",
      "user" => "fluentd",
      "argv" => %w[ tail -f /var/log/fluentd/fluentd.log ]
    }
    expected = {
      "time" => now.to_i,
      "pwd" => "/home/yugui",
      "user" => "fluentd",
      "argv" => %w[ tail -f /var/log/fluentd/fluentd.log ]
    }

    driver = create_driver(<<-CONFIG)
      table foo_%Y_%m_%d
      email foo@bar.example
      private_key_path /path/to/key
      project yourproject_id
      dataset yourdataset_id

      <inject>
      time_format %s
      time_key  time
      </inject>

      fetch_schema true
      fetch_schema_table foo
      schema [{"name": "time", "type": "INTEGER"}]

      <buffer time>
        timekey 1d
      </buffer>
    CONFIG

    writer = stub_writer(driver)
    mock(writer).fetch_schema('yourproject_id', 'yourdataset_id', 'foo') do
      sudo_schema_response.deep_stringify_keys["schema"]["fields"]
    end

    buf = nil
    driver.run { buf = driver.instance.format("my.tag", now, input) }

    assert_equal expected, MultiJson.load(buf)

    table_schema = driver.instance.instance_eval{ get_schema('yourproject_id', 'yourdataset_id', 'foo') }
    assert table_schema["time"]
    assert_equal :timestamp, table_schema["time"].type
    assert_equal :required, table_schema["time"].mode

    assert table_schema["tty"]
    assert_equal :string, table_schema["tty"].type
    assert_equal :nullable, table_schema["tty"].mode

    assert table_schema["pwd"]
    assert_equal :string, table_schema["pwd"].type
    assert_equal :required, table_schema["pwd"].mode

    assert table_schema["user"]
    assert_equal :string, table_schema["user"].type
    assert_equal :required, table_schema["user"].mode

    assert table_schema["argv"]
    assert_equal :string, table_schema["argv"].type
    assert_equal :repeated, table_schema["argv"].mode
  end

  def test__write_with_insert_id
    now = Time.now.to_i
    input = {
      "uuid" => "9ABFF756-0267-4247-847F-0895B65F0938",
    }
    expected = {
      insert_id: "9ABFF756-0267-4247-847F-0895B65F0938",
      json: {
        uuid: "9ABFF756-0267-4247-847F-0895B65F0938",
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
    mock(driver.instance).insert("yourproject_id", "yourdataset_id", "foo", [expected], instance_of(Fluent::BigQuery::RecordSchema), nil)

    driver.run do
      driver.feed('tag', now, input)
    end
  end

  def test__write_with_nested_insert_id
    input = {
      "data" => {
        "uuid" => "809F6BA7-1C16-44CD-9816-4B20E2C7AA2A",
      },
    }
    expected = {
      insert_id: "809F6BA7-1C16-44CD-9816-4B20E2C7AA2A",
      json: {
        data: {
          uuid: "809F6BA7-1C16-44CD-9816-4B20E2C7AA2A",
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

    mock(driver.instance).insert("yourproject_id", "yourdataset_id", "foo", [expected], instance_of(Fluent::BigQuery::RecordSchema), nil)

    driver.run do
      driver.feed('tag', Fluent::EventTime.now, input)
    end
  end

  def test_replace_record_key
    now = Fluent::EventTime.now
    input = {
      "vhost" => :bar,
      "@referer" => "http://referer.example",
      "bot_access" => true,
      "login-session" => false
    }
    expected = {
      "time" => now.to_i,
      "vhost" => "bar",
      "referer" => "http://referer.example",
      "bot_access" => true,
      "login_session" => false
    }

    driver = create_driver(<<-CONFIG)
      table foo
      email foo@bar.example
      private_key_path /path/to/key
      project yourproject_id
      dataset yourdataset_id

      replace_record_key true
      replace_record_key_regexp1 - _

      <inject>
      time_format %s
      time_key time
      </inject>

      schema [
        {"name": "time", "type": "INTEGER"},
        {"name": "vhost", "type": "STRING"},
        {"name": "refere", "type": "STRING"},
        {"name": "bot_access", "type": "BOOLEAN"},
        {"name": "login_session", "type": "BOOLEAN"}
      ]
    CONFIG

    buf = nil
    driver.run { buf = driver.instance.format("my.tag", now, input) }

    assert_equal expected, MultiJson.load(buf)
  end

  def test_convert_hash_to_json
    now = Fluent::EventTime.now
    input = {
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
    expected = {
      "time" => now.to_i,
      "vhost" => "bar",
      "referer" => "http://referer.example",
      "bot_access" => true,
      "loginsession" => false,
      "remote" => "{\"host\":\"remote.example\",\"ip\":\"192.0.2.1\",\"port\":12345,\"user\":\"tagomoris\"}"
    }

    driver = create_driver(<<-CONFIG)
      table foo
      email foo@bar.example
      private_key_path /path/to/key
      project yourproject_id
      dataset yourdataset_id

      convert_hash_to_json true

      <inject>
      time_format %s
      time_key time
      </inject>

      schema [
        {"name": "time", "type": "INTEGER"},
        {"name": "vhost", "type": "STRING"},
        {"name": "refere", "type": "STRING"},
        {"name": "bot_access", "type": "BOOLEAN"},
        {"name": "loginsession", "type": "BOOLEAN"}
      ]
    CONFIG

    buf = nil
    driver.run { buf = driver.instance.format("my.tag", now, input) }

    assert_equal expected, MultiJson.load(buf)
  end

  def test_write
    entry = {a: "b"}
    driver = create_driver

    writer = stub_writer(driver)
    mock.proxy(writer).insert_rows('yourproject_id', 'yourdataset_id', 'foo', [{json: hash_including(entry)}], template_suffix: nil)
    mock(writer.client).insert_all_table_data('yourproject_id', 'yourdataset_id', 'foo', {
      rows: [{json: hash_including(entry)}],
      skip_invalid_rows: false,
      ignore_unknown_values: false
    }, {options: {timeout_sec: nil, open_timeout_sec: 60}}) do
      s = stub!
      s.insert_errors { nil }
      s
    end

    driver.run do
      driver.feed("tag", Time.now.to_i, {"a" => "b"})
    end
  end

  def test_write_with_retryable_error
    driver = create_driver(<<-CONFIG)
      table foo
      email foo@bar.example
      private_key_path /path/to/key
      project yourproject_id
      dataset yourdataset_id

      <inject>
      time_format %s
      time_key  time
      </inject>

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

    entry = {a: "b"}
    writer = stub_writer(driver)
    mock(writer.client).insert_all_table_data('yourproject_id', 'yourdataset_id', 'foo', {
      rows: [{json: hash_including(entry)}],
      skip_invalid_rows: false,
      ignore_unknown_values: false
    }, {options: {timeout_sec: nil, open_timeout_sec: 60}}) do
      ex = Google::Apis::ServerError.new("error", status_code: 500)
      raise ex
    end

    assert_raise(Fluent::BigQuery::RetryableError) do
      driver.run do
        driver.feed("tag", Time.now.to_i, {"a" => "b"})
      end
    end
  end

  def test_write_with_not_retryable_error
    driver = create_driver(<<-CONFIG)
      table foo
      email foo@bar.example
      private_key_path /path/to/key
      project yourproject_id
      dataset yourdataset_id

      <inject>
      time_format %s
      time_key  time
      </inject>

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

    entry = {a: "b"}
    writer = stub_writer(driver)
    mock(writer.client).insert_all_table_data('yourproject_id', 'yourdataset_id', 'foo', {
      rows: [{json: hash_including(entry)}],
      skip_invalid_rows: false,
      ignore_unknown_values: false
    }, {options: {timeout_sec: nil, open_timeout_sec: 60}}) do
      ex = Google::Apis::ServerError.new("error", status_code: 501)
      def ex.reason
        "invalid"
      end
      raise ex
    end

    driver.instance_start
    tag, time, record = "tag", Time.now.to_i, {"a" => "b"}
    metadata = driver.instance.metadata_for_test(tag, time, record)
    chunk = driver.instance.buffer.generate_chunk(metadata).tap do |c|
      c.append([driver.instance.format(tag, time, record)])
    end
    assert_raise Fluent::BigQuery::UnRetryableError do
      driver.instance.write(chunk)
    end
    assert_in_delta driver.instance.retry.secondary_transition_at , Time.now, 0.1
    driver.instance_shutdown
  end

  def test_write_for_load
    schema_path = File.join(File.dirname(__FILE__), "testdata", "sudo.schema")
    driver = create_driver(<<-CONFIG)
      method load
      table foo
      email foo@bar.example
      private_key_path /path/to/key
      project yourproject_id
      dataset yourdataset_id

      <inject>
      time_format %s
      time_key  time
      </inject>

      schema_path #{schema_path}

      buffer_type memory
    CONFIG
    schema_fields = MultiJson.load(File.read(schema_path)).map(&:deep_symbolize_keys)

    writer = stub_writer(driver)
    io = StringIO.new("hello")
    mock(driver.instance).create_upload_source(is_a(Fluent::Plugin::Buffer::Chunk)).yields(io)
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

    driver.run do
      driver.feed("tag", Time.now.to_i, {"a" => "b"})
    end
  end

  def test_write_for_load_with_prevent_duplicate_load
    schema_path = File.join(File.dirname(__FILE__), "testdata", "sudo.schema")
    driver = create_driver(<<-CONFIG)
      method load
      table foo
      email foo@bar.example
      private_key_path /path/to/key
      project yourproject_id
      dataset yourdataset_id

      <inject>
      time_format %s
      time_key  time
      </inject>

      schema_path #{schema_path}
      prevent_duplicate_load true

      buffer_type memory
    CONFIG
    schema_fields = MultiJson.load(File.read(schema_path)).map(&:deep_symbolize_keys)

    io = StringIO.new("hello")
    mock(driver.instance).create_upload_source(is_a(Fluent::Plugin::Buffer::Chunk)).yields(io)
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

    driver.run do
      driver.feed("tag", Time.now.to_i, {"a" => "b"})
    end
  end

  def test_write_for_load_with_retryable_error
    schema_path = File.join(File.dirname(__FILE__), "testdata", "sudo.schema")
    driver = create_driver(<<-CONFIG)
      method load
      table foo
      email foo@bar.example
      private_key_path /path/to/key
      project yourproject_id
      dataset yourdataset_id

      <inject>
      time_format %s
      time_key  time
      </inject>

      schema_path #{schema_path}

      buffer_type memory
    CONFIG
    schema_fields = MultiJson.load(File.read(schema_path)).map(&:deep_symbolize_keys)

    driver.instance_start
    tag, time, record = "tag", Time.now.to_i, {"a" => "b"}
    metadata = driver.instance.metadata_for_test(tag, time, record)
    chunk = driver.instance.buffer.generate_chunk(metadata).tap do |c|
      c.append([driver.instance.format(tag, time, record)])
    end

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

    assert_raise Fluent::BigQuery::RetryableError do
      driver.instance.write(chunk)
    end
    driver.instance_shutdown
  end

  def test_write_for_load_with_not_retryable_error
    schema_path = File.join(File.dirname(__FILE__), "testdata", "sudo.schema")
    driver = create_driver(<<-CONFIG)
      method load
      table foo
      email foo@bar.example
      private_key_path /path/to/key
      project yourproject_id
      dataset yourdataset_id

      <inject>
      time_format %s
      time_key  time
      </inject>

      schema_path #{schema_path}

      buffer_type memory
      <secondary>
        type file
        path error
        utc
      </secondary>
    CONFIG
    schema_fields = MultiJson.load(File.read(schema_path)).map(&:deep_symbolize_keys)

    driver.instance_start
    tag, time, record = "tag", Time.now.to_i, {"a" => "b"}
    metadata = driver.instance.metadata_for_test(tag, time, record)
    chunk = driver.instance.buffer.generate_chunk(metadata).tap do |c|
      c.append([driver.instance.format(tag, time, record)])
    end

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

    assert_raise Fluent::BigQuery::UnRetryableError do
      driver.instance.write(chunk)
    end
    assert_in_delta driver.instance.retry.secondary_transition_at , Time.now, 0.1
    driver.instance_shutdown
  end

  def test_write_with_row_based_table_id_formatting
    entry = [
      {json: {a: "b", created_at: Time.local(2014,8,20,9,0,0).strftime("%Y_%m_%d")}},
    ]
    driver = create_driver(<<-CONFIG)
      <buffer created_at>
      </buffer>
      table foo_${created_at}
      email foo@bar.example
      private_key_path /path/to/key
      project yourproject_id
      dataset yourdataset_id

      schema [
        {"name": "time", "type": "INTEGER"}
      ]
    CONFIG

    writer = stub_writer(driver)
    mock(writer.client).insert_all_table_data('yourproject_id', 'yourdataset_id', 'foo_2014_08_20', {
      rows: [entry[0]],
      skip_invalid_rows: false,
      ignore_unknown_values: false
    }, {options: {timeout_sec: nil, open_timeout_sec: 60}}) { stub!.insert_errors { nil } }

    driver.run do
      driver.feed("tag", Time.now.to_i, {"a" => "b", "created_at" => Time.local(2014,8,20,9,0,0).strftime("%Y_%m_%d")})
    end
  end

  def test_auto_create_table_by_bigquery_api
    now = Time.at(Time.now.to_i)
    message = {
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

    driver = create_driver(<<-CONFIG)
      table foo
      email foo@bar.example
      private_key_path /path/to/key
      project yourproject_id
      dataset yourdataset_id

      <inject>
      time_format %s
      time_key  time
      </inject>

      auto_create_table true
      schema_path #{File.join(File.dirname(__FILE__), "testdata", "apache.schema")}
    CONFIG
    writer = stub_writer(driver)
    mock(writer).insert_rows('yourproject_id', 'yourdataset_id', 'foo', [{json: message.deep_symbolize_keys}], template_suffix: nil) do
      raise Fluent::BigQuery::RetryableError.new(nil, Google::Apis::ServerError.new("Not found: Table yourproject_id:yourdataset_id.foo", status_code: 404, body: "Not found: Table yourproject_id:yourdataset_id.foo"))
    end
    mock(writer).create_table('yourproject_id', 'yourdataset_id', 'foo', driver.instance.instance_variable_get(:@table_schema))

    assert_raise(RuntimeError) do
      driver.run do
        driver.feed("tag", Fluent::EventTime.from_time(now), message)
      end
    end
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
    mock(writer).insert_rows('yourproject_id', 'yourdataset_id', 'foo', [message], template_suffix: nil) do
      raise Fluent::BigQuery::RetryableError.new(nil, Google::Apis::ServerError.new("Not found: Table yourproject_id:yourdataset_id.foo", status_code: 404, body: "Not found: Table yourproject_id:yourdataset_id.foo"))
    end
    mock(writer).create_table('yourproject_id', 'yourdataset_id', 'foo', driver.instance.instance_variable_get(:@table_schema))

    assert_raise(RuntimeError) do
      driver.run do
        driver.feed("tag", Fluent::EventTime.now, message[:json])
      end
    end
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
