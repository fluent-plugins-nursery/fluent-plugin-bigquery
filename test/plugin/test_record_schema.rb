require 'helper'

class RecordSchemaTest < Test::Unit::TestCase
  def base_schema
    [
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
      },
      {
        "name" => "utilization",
        "type" => "NUMERIC",
        "mode" => "NULLABLE"
      },
      {
        "name" => "bigutilization",
        "type" => "BIGNUMERIC",
        "mode" => "NULLABLE"
      }
    ]
  end

  def base_schema_with_new_column
    [
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
      },
      {
        "name" => "utilization",
        "type" => "NUMERIC",
        "mode" => "NULLABLE"
      },
      {
        "name" => "bigutilization",
        "type" => "BIGNUMERIC",
        "mode" => "NULLABLE"
      },
      {
        "name" => "new_column",
        "type" => "STRING",
        "mode" => "REQUIRED"
      },
    ]
  end

  def base_schema_with_type_changed_column
    [
      {
        "name" => "time",
        "type" => "INTEGER", # change type
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
      },
      {
        "name" => "utilization",
        "type" => "NUMERIC",
        "mode" => "NULLABLE"
      },
      {
        "name" => "bigutilization",
        "type" => "BIGNUMERIC",
        "mode" => "NULLABLE"
      }
    ]
  end

  def test_load_schema
    fields = Fluent::BigQuery::RecordSchema.new("record")
    fields.load_schema(base_schema)
    assert { Fluent::BigQuery::Helper.deep_stringify_keys(fields.to_a) == base_schema }
  end

  def test_load_schema_allow_overwrite_with_type_changed_column
    fields = Fluent::BigQuery::RecordSchema.new("record")
    fields.load_schema(base_schema)

    fields.load_schema(base_schema_with_type_changed_column)
    assert { Fluent::BigQuery::Helper.deep_stringify_keys(fields.to_a) == base_schema_with_type_changed_column }
  end

  def test_load_schema_allow_overwrite_with_new_column
    fields = Fluent::BigQuery::RecordSchema.new("record")
    fields.load_schema(base_schema)

    fields.load_schema(base_schema_with_new_column)
    assert { Fluent::BigQuery::Helper.deep_stringify_keys(fields.to_a) == base_schema_with_new_column }
  end

  def test_format_one
    fields = Fluent::BigQuery::RecordSchema.new("record")
    fields.load_schema(base_schema)

    time = Time.local(2016, 2, 7, 19, 0, 0).utc

    formatted = fields.format_one({
      "time" => time, "tty" => nil, "pwd" => "/home", "user" => "joker1007", "argv" => ["foo", 42]
    })
    assert_equal(
      formatted,
      {
        "time" => time.strftime("%Y-%m-%d %H:%M:%S.%6L %:z"), "pwd" => "/home", "user" => "joker1007", "argv" => ["foo", "42"]
      }
    )
  end

  def test_format_one_convert_array_or_hash_to_json
    fields = Fluent::BigQuery::RecordSchema.new("record")
    fields.load_schema(base_schema)

    time = Time.local(2016, 2, 7, 19, 0, 0).utc

    formatted = fields.format_one({
      "time" => time, "tty" => ["tty1", "tty2", "tty3"], "pwd" => "/home", "user" => {name: "joker1007", uid: 10000}, "argv" => ["foo", 42], "utilization" => "0.837", "bigutilization" => "0.837"
    })
    assert_equal(
      formatted,
      {
        "time" => time.strftime("%Y-%m-%d %H:%M:%S.%6L %:z"), "tty" => MultiJson.dump(["tty1", "tty2", "tty3"]), "pwd" => "/home", "user" => MultiJson.dump({name: "joker1007", uid: 10000}), "argv" => ["foo", "42"], "utilization" => "0.837", "bigutilization" => "0.837"
      }
    )
  end

  def test_format_one_with_extra_column
    fields = Fluent::BigQuery::RecordSchema.new("record")
    fields.load_schema(base_schema)

    time = Time.local(2016, 2, 7, 19, 0, 0).utc

    formatted = fields.format_one({
      "time" => time, "tty" => nil, "pwd" => "/home", "user" => "joker1007", "argv" => ["foo", 42.195], "extra" => "extra_data"
    })
    assert_equal(
      formatted,
      {
        "time" => time.strftime("%Y-%m-%d %H:%M:%S.%6L %:z"), "pwd" => "/home", "user" => "joker1007", "argv" => ["foo", "42.195"], "extra" => "extra_data"
      }
    )
  end
end
