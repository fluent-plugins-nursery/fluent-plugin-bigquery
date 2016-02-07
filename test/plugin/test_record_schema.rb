require 'helper'
require 'active_support/json'
require 'active_support/core_ext/hash'
require 'active_support/core_ext/object/json'

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
        "name" => "new_column",
        "type" => "STRING",
        "mode" => "REQUIRED"
      }
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
    ]
  end

  def test_load_schema
    fields = Fluent::BigQueryOutput::RecordSchema.new("record")
    fields.load_schema(base_schema, true)
    assert { fields.to_a.as_json == base_schema }
  end

  def test_load_schema_allow_overwrite_with_type_changed_column
    fields = Fluent::BigQueryOutput::RecordSchema.new("record")
    fields.load_schema(base_schema, true)

    fields.load_schema(base_schema_with_type_changed_column, true)
    assert { fields.to_a.as_json == base_schema_with_type_changed_column }
  end

  def test_load_schema_allow_overwrite_with_new_column
    fields = Fluent::BigQueryOutput::RecordSchema.new("record")
    fields.load_schema(base_schema, true)

    fields.load_schema(base_schema_with_new_column, true)
    assert { fields.to_a.as_json == base_schema_with_new_column }
  end

  def test_load_schema_not_allow_overwrite_with_type_changed_column
    fields = Fluent::BigQueryOutput::RecordSchema.new("record")
    fields.load_schema(base_schema, false)

    fields.load_schema(base_schema_with_type_changed_column, false)
    assert { fields.to_a.as_json == base_schema }
  end

  def test_load_schema_no_allow_overwrite_with_new_column
    fields = Fluent::BigQueryOutput::RecordSchema.new("record")
    fields.load_schema(base_schema, false)

    fields.load_schema(base_schema_with_new_column, false)
    assert { fields.to_a.as_json == base_schema_with_new_column }
  end
end
