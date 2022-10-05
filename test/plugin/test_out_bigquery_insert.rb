require 'helper'

class BigQueryInsertOutputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
  end

  def is_ruby2?
    RUBY_VERSION.to_i < 3
  end

  def build_args(args)
    if is_ruby2?
      args << {}
    end
    args
  end

  SCHEMA_PATH = File.join(File.dirname(__FILE__), "testdata", "apache.schema")

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
    Fluent::Test::Driver::Output.new(Fluent::Plugin::BigQueryInsertOutput).configure(conf)
  end

  def stub_writer(stub_auth: true)
    stub.proxy(Fluent::BigQuery::Writer).new.with_any_args do |writer|
      stub(writer).get_auth { nil } if stub_auth
      yield writer
      writer
    end
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

      insert_id_field $.data.uuid
      schema [{"name": "data", "type": "RECORD", "fields": [
        {"name": "uuid", "type": "STRING"}
      ]}]
    CONFIG

    mock(driver.instance).insert("yourproject_id", "yourdataset_id", "foo", [expected], instance_of(Fluent::BigQuery::RecordSchema), nil)

    driver.run do
      driver.feed('tag', Fluent::EventTime.now, input)
    end
  end

  def test_write
    entry = {a: "b"}
    driver = create_driver

    stub_writer do |writer|
      args = build_args(['yourproject_id', 'yourdataset_id', 'foo', {
        rows: [{json: hash_including(entry)}],
        skip_invalid_rows: false,
        ignore_unknown_values: false
      }])
      mock(writer.client).insert_all_table_data(*args) do
        s = stub!
        s.insert_errors { nil }
        s
      end
    end

    driver.run do
      driver.feed("tag", Time.now.to_i, {"a" => "b"})
    end
  end

  def test_write_with_retryable_error
    data_input = [
      { "status_code" => 500  },
      { "status_code" => 502  },
      { "status_code" => 503  },
      { "status_code" => 504  },
    ]

    data_input.each do |d|
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
      stub_writer do |writer|
        args = build_args(['yourproject_id', 'yourdataset_id', 'foo', {
          rows: [{json: hash_including(entry)}],
          skip_invalid_rows: false,
          ignore_unknown_values: false
        }])
        mock(writer.client).insert_all_table_data(*args) do
          ex = Google::Apis::ServerError.new("error", status_code: d["status_code"])
          raise ex
        end
      end

      assert_raise(Fluent::BigQuery::RetryableError) do
        driver.run do
          driver.feed("tag", Time.now.to_i, {"a" => "b"})
        end
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
    stub_writer do |writer|
      args = build_args(['yourproject_id', 'yourdataset_id', 'foo', {
        rows: [{json: hash_including(entry)}],
        skip_invalid_rows: false,
        ignore_unknown_values: false
      }])
      mock(writer.client).insert_all_table_data(*args) do
        ex = Google::Apis::ServerError.new("error", status_code: 501)
        def ex.reason
          "invalid"
        end
        raise ex
      end
    end

    driver.instance_start
    tag, time, record = "tag", Time.now.to_i, {"a" => "b"}
    metadata = Fluent::Plugin::Buffer::Metadata.new(tag, time, record)
    chunk = driver.instance.buffer.generate_chunk(metadata).tap do |c|
      c.append([driver.instance.format(tag, time, record)])
    end
    assert_raise Fluent::BigQuery::UnRetryableError do
      driver.instance.write(chunk)
    end
    assert_in_delta driver.instance.retry.secondary_transition_at , Time.now, 0.2
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

    stub_writer do |writer|
      args = ['yourproject_id', 'yourdataset_id', 'foo_2014_08_20', {
        rows: [entry[0]],
        skip_invalid_rows: false,
        ignore_unknown_values: false
      }]
      if RUBY_VERSION.to_i < 3
        args << {}
      end
      mock(writer.client).insert_all_table_data(*args) { stub!.insert_errors { nil } }
    end

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

    schema_fields = Fluent::BigQuery::Helper.deep_symbolize_keys(MultiJson.load(File.read(SCHEMA_PATH)))

    stub_writer do |writer|
      body = {
        rows: [{json: Fluent::BigQuery::Helper.deep_symbolize_keys(message)}],
        skip_invalid_rows: false,
        ignore_unknown_values: false,
      }
      args = build_args(['yourproject_id', 'yourdataset_id', 'foo', body])
      mock(writer.client).insert_all_table_data(*args) do
        raise Google::Apis::ClientError.new("notFound: Not found: Table yourproject_id:yourdataset_id.foo", status_code: 404)
      end.at_least(1)
      mock(writer).sleep(instance_of(Numeric)) { nil }.at_least(1)

      args = build_args(['yourproject_id', 'yourdataset_id', {
        table_reference: {
          table_id: 'foo',
        },
        schema: {
          fields: schema_fields,
        },
      }])
      mock(writer.client).insert_table(*args)
    end

    assert_raise(RuntimeError) do
      driver.run do
        driver.feed("tag", Fluent::EventTime.from_time(now), message)
      end
    end
  end

  def test_auto_create_partitioned_table_by_bigquery_api
    now = Time.now
    message = {
      json: {
        time: now.to_i,
        request: {
          vhost: "bar",
          path: "/path/to/baz",
          method: "GET",
          protocol: "HTTP/1.0",
          agent: "libwww",
          referer: "http://referer.example",
          time: (now - 1).to_f,
          bot_access: true,
          loginsession: false,
        },
        remote: {
          host: "remote.example",
          ip: "192.168.1.1",
          user: "nagachika",
        },
        response: {
          status: 200,
          bytes: 72,
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

      time_partitioning_type day
      time_partitioning_field time
      time_partitioning_expiration 1h

      require_partition_filter true
    CONFIG

    schema_fields = Fluent::BigQuery::Helper.deep_symbolize_keys(MultiJson.load(File.read(SCHEMA_PATH)))

    stub_writer do |writer|
      body = {
        rows: [message],
        skip_invalid_rows: false,
        ignore_unknown_values: false,
      }
      args = build_args(['yourproject_id', 'yourdataset_id', 'foo', body])
      mock(writer.client).insert_all_table_data(*args) do
        raise Google::Apis::ClientError.new("notFound: Not found: Table yourproject_id:yourdataset_id.foo", status_code: 404)
      end.at_least(1)
      mock(writer).sleep(instance_of(Numeric)) { nil }.at_least(1)

      args = build_args(['yourproject_id', 'yourdataset_id', {
        table_reference: {
          table_id: 'foo',
        },
        schema: {
          fields: schema_fields,
        },
        time_partitioning: {
          type: 'DAY',
          field: 'time',
          expiration_ms: 3600000,
        },
        require_partition_filter: true,
      }])
      mock(writer.client).insert_table(*args)
    end

    assert_raise(RuntimeError) do
      driver.run do
        driver.feed("tag", Fluent::EventTime.now, message[:json])
      end
    end
  end

  def test_auto_create_clustered_table_by_bigquery_api
    now = Time.now
    message = {
      json: {
        time: now.to_i,
        request: {
          vhost: "bar",
          path: "/path/to/baz",
          method: "GET",
          protocol: "HTTP/1.0",
          agent: "libwww",
          referer: "http://referer.example",
          time: (now - 1).to_f,
          bot_access: true,
          loginsession: false,
        },
        remote: {
          host: "remote.example",
          ip: "192.168.1.1",
          user: "nagachika",
        },
        response: {
          status: 200,
          bytes: 72,
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

      time_partitioning_type day
      time_partitioning_field time
      time_partitioning_expiration 1h

      clustering_fields [
        "time",
        "vhost"
      ]
    CONFIG

    schema_fields = Fluent::BigQuery::Helper.deep_symbolize_keys(MultiJson.load(File.read(SCHEMA_PATH)))

    stub_writer do |writer|
      body = {
        rows: [message],
        skip_invalid_rows: false,
        ignore_unknown_values: false,
      }
      args = build_args(['yourproject_id', 'yourdataset_id', 'foo', body])
      mock(writer.client).insert_all_table_data(*args) do
        raise Google::Apis::ClientError.new("notFound: Not found: Table yourproject_id:yourdataset_id.foo", status_code: 404)
      end.at_least(1)
      mock(writer).sleep(instance_of(Numeric)) { nil }.at_least(1)

      args = build_args(['yourproject_id', 'yourdataset_id', {
        table_reference: {
          table_id: 'foo',
        },
        schema: {
          fields: schema_fields,
        },
        time_partitioning: {
          type: 'DAY',
          field: 'time',
          expiration_ms: 3600000,
        },
        clustering: {
          fields: [
            'time',
            'vhost',
          ],
        },
      }])
      mock(writer.client).insert_table(*args)
    end

    assert_raise(RuntimeError) do
      driver.run do
        driver.feed("tag", Fluent::EventTime.now, message[:json])
      end
    end
  end
end
