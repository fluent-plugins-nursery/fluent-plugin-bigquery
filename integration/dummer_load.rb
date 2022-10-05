require "time"

configure "load" do
  host "localhost"
  port 24224
  rate 100
  tag type: :string, any: %w(load_data)
  field :id, type: :integer, countup: true
  field :string_field, type: :string, any: %w(str1 str2 str3 str4)
  field :timestamp_field, type: :string, value: Time.now.iso8601
  field :date, type: :string, value: Time.now.strftime("%Y-%m-%d")
end
