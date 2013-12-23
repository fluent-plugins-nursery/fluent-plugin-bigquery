# -*- coding: utf-8 -*-
require 'helper'
require 'json'
require 'tempfile'

class LoadRequestBodyWrapperTest < Test::Unit::TestCase
  def content_alphabet(repeat)
    (0...repeat).map{|i| "#{i}0123456789\n" }.join
  end

  def content_kana(repeat)
    (0...repeat).map{|i| "#{i}あいうえおかきくけこ\n" }.join
  end

  def mem_chunk(repeat=10, kana=false)
    content = kana ? content_kana(repeat) : content_alphabet(repeat)
    Fluent::MemoryBufferChunk.new('bc_mem', content)
  end

  def file_chunk(repeat=10, kana=false)
    content = kana ? content_kana(repeat) : content_alphabet(repeat)
    tmpfile = Tempfile.new('fluent_bigquery_plugin_test')
    buf = Fluent::FileBufferChunk.new('bc_mem', tmpfile.path, tmpfile.object_id)
    buf << content
    buf
  end

  def field_defs
    [{"name" => "field1", "type" => "STRING"}, {"name" => "field2", "type" => "INTEGER"}]
  end

  def check_meta(blank, first, last)
    assert_equal "", blank

    header1, body1 = first.split("\n\n")
    assert_equal "Content-Type: application/json; charset=UTF-8", header1
    metadata = JSON.parse(body1)
    assert_equal "<required for JSON files>", metadata["configuration"]["load"]["sourceFormat"]
    assert_equal "field1", metadata["configuration"]["load"]["schema"]["fields"][0]["name"]
    assert_equal "STRING", metadata["configuration"]["load"]["schema"]["fields"][0]["type"]
    assert_equal "field2", metadata["configuration"]["load"]["schema"]["fields"][1]["name"]
    assert_equal "INTEGER", metadata["configuration"]["load"]["schema"]["fields"][1]["type"]
    assert_equal "pname1", metadata["configuration"]["load"]["destinationTable"]["projectId"]
    assert_equal "dname1", metadata["configuration"]["load"]["destinationTable"]["datasetId"]
    assert_equal "tname1", metadata["configuration"]["load"]["destinationTable"]["tableId"]

    assert_equal "--\n", last
  end

  def check_ascii(data)
    blank, first, second, last = data.split(/--xxx\n?/)

    check_meta(blank, first, last)

    header2, body2 = second.split("\n\n")
    assert_equal "Content-Type: application/octet-stream", header2
    i = 0
    body2.each_line do |line|
      assert_equal "#{i}0123456789\n", line
      i += 1
    end
  end

  def check_kana(data)
    blank, first, second, last = data.split(/--xxx\n?/)

    check_meta(blank, first, last)

    header2, body2 = second.split("\n\n")
    assert_equal "Content-Type: application/octet-stream", header2
    i = 0
    body2.each_line do |line|
      assert_equal "#{i}あいうえおかきくけこ\n", line
      i += 1
    end
  end

  def setup
    @klass = Fluent::BigQueryPlugin::LoadRequestBodyWrapper
    self
  end

  def test_memory_buf
    d1 = @klass.new('pname1', 'dname1', 'tname1', field_defs(), mem_chunk(10))
    data1 = d1.read.force_encoding("UTF-8")
    check_ascii(data1)

    d2 = @klass.new('pname1', 'dname1', 'tname1', field_defs(), mem_chunk(10))
    data2 = ""
    while !d2.eof? do
      buf = "     "
      objid = buf.object_id
      data2 << d2.read(20, buf)
      assert_equal objid, buf.object_id
    end
    data2.force_encoding("UTF-8")

    assert_equal data1.size, data2.size
  end

  def test_memory_buf2
    d1 = @klass.new('pname1', 'dname1', 'tname1', field_defs(), mem_chunk(100000))
    data1 = d1.read.force_encoding("UTF-8")
    check_ascii(data1)

    d2 = @klass.new('pname1', 'dname1', 'tname1', field_defs(), mem_chunk(100000))
    data2 = ""
    while !d2.eof? do
      buf = "     "
      objid = buf.object_id
      data2 << d2.read(2048, buf)
      assert_equal objid, buf.object_id
    end
    data2.force_encoding("UTF-8")

    assert_equal data1.size, data2.size
  end

  def test_memory_buf3 # kana
    d1 = @klass.new('pname1', 'dname1', 'tname1', field_defs(), mem_chunk(100000, true))
    data1 = d1.read.force_encoding("UTF-8")
    check_kana(data1)

    d2 = @klass.new('pname1', 'dname1', 'tname1', field_defs(), mem_chunk(100000, true))
    data2 = ""
    while !d2.eof? do
      buf = "     "
      objid = buf.object_id
      data2 << d2.read(2048, buf)
      assert_equal objid, buf.object_id
    end
    data2.force_encoding("UTF-8")

    assert_equal data1.size, data2.size
  end

  def test_file_buf
    d1 = @klass.new('pname1', 'dname1', 'tname1', field_defs(), file_chunk(10))
    data1 = d1.read.force_encoding("UTF-8")
    check_ascii(data1)

    d2 = @klass.new('pname1', 'dname1', 'tname1', field_defs(), file_chunk(10))
    data2 = ""
    while !d2.eof? do
      buf = "     "
      objid = buf.object_id
      data2 << d2.read(20, buf)
      assert_equal objid, buf.object_id
    end
    data2.force_encoding("UTF-8")

    assert_equal data1.size, data2.size
  end

  def test_file_buf2
    d1 = @klass.new('pname1', 'dname1', 'tname1', field_defs(), file_chunk(100000))
    data1 = d1.read.force_encoding("UTF-8")
    check_ascii(data1)

    d2 = @klass.new('pname1', 'dname1', 'tname1', field_defs(), file_chunk(100000))
    data2 = ""
    while !d2.eof? do
      buf = "     "
      objid = buf.object_id
      data2 << d2.read(20480, buf)
      assert_equal objid, buf.object_id
    end
    data2.force_encoding("UTF-8")

    assert_equal data1.size, data2.size
  end

  def test_file_buf3 # kana
    d1 = @klass.new('pname1', 'dname1', 'tname1', field_defs(), file_chunk(100000, true))
    data1 = d1.read.force_encoding("UTF-8")
    check_kana(data1)

    d2 = @klass.new('pname1', 'dname1', 'tname1', field_defs(), file_chunk(100000, true))
    data2 = ""
    while !d2.eof? do
      buf = "     "
      objid = buf.object_id
      data2 << d2.read(20480, buf)
      assert_equal objid, buf.object_id
    end
    data2.force_encoding("UTF-8")

    assert_equal data1.size, data2.size
  end
end
