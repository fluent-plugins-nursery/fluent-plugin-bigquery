require 'helper'
require 'google/api_client'
require 'fluent/plugin/buf_memory'

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
    Fluent::Test::OutputTestDriver.new(Fluent::BigQueryOutput).configure(conf)
  end

  def stub_client(driver)
    stub(client = Object.new) do |expect|
      expect.discovered_api("bigquery", "v2") { stub! }
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

  def test_configure_auth
    key = stub!
    mock(Google::APIClient::PKCS12).load_key('/path/to/key', 'notasecret') { key }
    authorization = Object.new
    asserter = mock!.authorize { authorization }
    mock(Google::APIClient::JWTAsserter).new('foo@bar.example', API_SCOPE, key) { asserter }

    mock.proxy(Google::APIClient).new.with_any_args { 
      mock!.__send__(:authorization=, authorization) {}
    }

    driver = create_driver(CONFIG)
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
        "remote" => {
          "host" => "remote.example",
          "ip" =>  "192.0.2.1",
          "user" => "tagomoris",
        }
      }
    }

    driver = create_driver(CONFIG)
    mock_client(driver) do |expect|
      expect.discovered_api("bigquery", "v2") { stub! }
    end
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
          "user" => "tagomoris",
        },
        "response" => {
          "status" => 1,
          "bytes" => 3,
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
    mock_client(driver) do |expect|
      expect.discovered_api("bigquery", "v2") { stub! }
    end
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
    mock_client(driver) do |expect|
      expect.discovered_api("bigquery", "v2") { stub! }
    end
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
      expect.discovered_api("bigquery", "v2") { mock!.tables.mock!.get { Object.new } }
      expect.execute(
        :api_method => anything,
        :parameters => {
          'projectId' => 'yourproject_id',
          'datasetId' => 'yourdataset_id',
          'tableId' => 'foo'
        }
      ) {
        s = stub!
        s.success? { true }
        s.body { JSON.generate(sudo_schema_response) }
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
      expect.discovered_api("bigquery", "v2") { mock!.tables.mock!.get { Object.new } }
      expect.execute(
        :api_method => anything,
        :parameters => {
          'projectId' => 'yourproject_id',
          'datasetId' => 'yourdataset_id',
          'tableId' => now.strftime('foo_%Y_%m_%d')
        }
      ) {
        s = stub!
        s.success? { true }
        s.body { JSON.generate(sudo_schema_response) }
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
      "insertId" => "9ABFF756-0267-4247-847F-0895B65F0938",
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
    mock_client(driver) do |expect|
      expect.discovered_api("bigquery", "v2") { stub! }
    end
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
      "insertId" => "809F6BA7-1C16-44CD-9816-4B20E2C7AA2A",
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
    mock_client(driver) do |expect|
      expect.discovered_api("bigquery", "v2") { stub! }
    end
    driver.instance.start
    buf = driver.instance.format_stream("my.tag", [input])
    driver.instance.shutdown

    assert_equal expected, MessagePack.unpack(buf)
  end

  def test_empty_value_in_required
    now = Time.now
    input = [
      now,
      {
        "tty" => "pts/1",
        "pwd" => "/home/yugui",
        "user" => nil,
        "argv" => %w[ tail -f /var/log/fluentd/fluentd.log ]
      }
    ]

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
    mock_client(driver) do |expect|
      expect.discovered_api("bigquery", "v2") { stub! }
    end
    driver.instance.start
    assert_raises(RuntimeError.new("Required field user cannot be null")) do
      driver.instance.format_stream("my.tag", [input])
    end
    driver.instance.shutdown
  end

  def test_write
    entry = {"json" => {"a" => "b"}}, {"json" => {"b" => "c"}}
    driver = create_driver(CONFIG)
    mock_client(driver) do |expect|
      expect.discovered_api("bigquery", "v2") { mock!.tabledata.mock!.insert_all { Object.new } }
      expect.execute(
        :api_method => anything,
        :parameters => {
          'projectId' => 'yourproject_id',
          'datasetId' => 'yourdataset_id',
          'tableId' => 'foo',
        },
        :body_object => {
          'rows' => [entry]
        }
      ) { stub!.success? { true } }
    end

    chunk = Fluent::MemoryBufferChunk.new("my.tag")
    chunk << entry.to_msgpack

    driver.instance.start
    driver.instance.write(chunk)
    driver.instance.shutdown
  end

  def test_generate_table_id
    driver = create_driver
    table_id_format = 'foo_%Y_%m_%d'
    time = Time.local(2014, 8, 11, 21, 20, 56)
    table_id = driver.instance.generate_table_id(table_id_format, time)
    assert_equal 'foo_2014_08_11', table_id
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
    }

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
      expect.discovered_api("bigquery", "v2") {
        mock! {
          tables.mock!.insert { Object.new }
          tabledata.mock!.insert_all { Object.new }
        }
      }
      expect.execute(
        :api_method => anything,
        :parameters => {
          'projectId' => 'yourproject_id',
          'datasetId' => 'yourdataset_id',
          'tableId' => 'foo'
        },
        :body_object => {
          "rows" => [ message ]
        }
      ) {
        s = stub!
        s.success? { false }
        s.body { JSON.generate({
          'error' => { "code" => 404, "message" => "Not Found: Table yourproject_id:yourdataset_id.foo" }
        }) }
        s.status { 404 }
        s
      }
      expect.execute(
        :api_method => anything,
        :parameters => {
          'projectId' => 'yourproject_id',
          'datasetId' => 'yourdataset_id',
        },
        :body_object => {
          'tableReference' => {
            'tableId' => 'foo',
          },
          'schema' => {
            'fields' => JSON.parse(File.read(File.join(File.dirname(__FILE__), "testdata", "apache.schema")))
          }
        }
      ) {
        s = stub!
        s.success? { true }
        s
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
