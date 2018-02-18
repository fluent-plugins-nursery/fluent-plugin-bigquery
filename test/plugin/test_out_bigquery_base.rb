require 'helper'

class BigQueryBaseOutputTest < Test::Unit::TestCase
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
    Fluent::Test::Driver::Output.new(Fluent::Plugin::BigQueryBaseOutput).configure(conf)
  end

  def stub_writer(driver, stub_auth: true)
    stub.proxy(Fluent::BigQuery::Writer).new.with_any_args do |writer|
      stub(writer).get_auth { nil } if stub_auth
      yield writer
      writer
    end
  end

  private def sudo_schema_response
    {
      "schema" => {
        "fields" => [
          {
            "name" => "time",
            "type" => "TIMESTAMP",
            "mode" => "REQUIRED"
          },
          {
            "name" => "tty",
            "type" => "STRING",
            "mode" => "NULLABLE"
          },
          {
            "name" => "pwd",
            "type" => "STRING",
            "mode" => "REQUIRED"
          },
          {
            "name" => "user",
            "type" => "STRING",
            "mode" => "REQUIRED"
          },
          {
            "name" => "argv",
            "type" => "STRING",
            "mode" => "REPEATED"
          }
        ]
      }
    }
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
    driver = create_driver
    stub_writer(driver, stub_auth: false) do |writer|
      mock(writer).get_auth_from_private_key { stub! }
    end
    assert driver.instance.writer.client.is_a?(Google::Apis::BigqueryV2::BigqueryService)
  end

  def test_configure_auth_compute_engine
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

    stub_writer(driver, stub_auth: false) do |writer|
      mock(writer).get_auth_from_compute_engine { stub! }
    end
    assert driver.instance.writer.client.is_a?(Google::Apis::BigqueryV2::BigqueryService)
  end

  def test_configure_auth_json_key_as_file
    driver = create_driver(%[
      table foo
      auth_method json_key
      json_key jsonkey.josn
      project yourproject_id
      dataset yourdataset_id
      schema [
        {"name": "time", "type": "INTEGER"},
        {"name": "status", "type": "INTEGER"},
        {"name": "bytes", "type": "INTEGER"}
      ]
    ])

    stub_writer(driver, stub_auth: false) do |writer|
      mock(writer).get_auth_from_json_key { stub! }
    end
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
    stub(Google::Auth::ServiceAccountCredentials).make_creds(json_key_io: satisfy {|arg| JSON.parse(arg.read) == JSON.parse(json_key_io.read) }, scope: API_SCOPE) { authorization }

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
    stub_writer(driver, stub_auth: false) do |writer|
      mock.proxy(writer).get_auth_from_json_key { stub! }
    end
    assert driver.instance.writer.client.is_a?(Google::Apis::BigqueryV2::BigqueryService)
  end

  def test_configure_auth_application_default
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

    stub_writer(driver, stub_auth: false) do |writer|
      mock.proxy(writer).get_auth_from_application_default { stub! }
    end
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
      "%s.%6N",
      lambda{|t| t.strftime("%s.%6N").to_f },
      lambda{|recv, expected, actual|
        recv.assert_in_delta(expected, actual, Float::EPSILON / 10**3)
      }
    ],
    [
      "%Y-%m-%dT%H:%M:%S%:z",
      lambda{|t| t.iso8601 },
      :assert_equal.to_proc
    ],
  ].each do |format, expect_time, assert|
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

    stub_writer(driver) do |writer|
      mock(writer).fetch_schema('yourproject_id', 'yourdataset_id', 'foo') do
        sudo_schema_response["schema"]["fields"]
      end
    end

    buf = nil
    driver.run { buf = driver.instance.format("my.tag", now, input) }

    assert_equal expected, MultiJson.load(buf)

    table_schema = driver.instance.instance_eval{ @fetched_schemas['yourproject_id.yourdataset_id.foo'] }
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

    stub_writer(driver) do |writer|
      mock(writer).fetch_schema('yourproject_id', 'yourdataset_id', 'foo') do
        sudo_schema_response["schema"]["fields"]
      end
    end

    buf = nil
    driver.run { buf = driver.instance.format("my.tag", now, input) }

    assert_equal expected, MultiJson.load(buf)

    table_schema = driver.instance.instance_eval{ @fetched_schemas['yourproject_id.yourdataset_id.foo'] }
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
end
