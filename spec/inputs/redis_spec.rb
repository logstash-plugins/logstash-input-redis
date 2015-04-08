require "logstash/devutils/rspec/spec_helper"
require "redis"
require "stud/try"

def populate(key, event_count)
  require "logstash/event"
  redis = Redis.new(:host => "localhost")
  event_count.times do |value|
    event = LogStash::Event.new("sequence" => value)
    Stud.try(10.times) do
      redis.rpush(key, event.to_json)
    end
  end
end

def process(conf, event_count)
  events = input(conf) do |pipeline, queue|
    event_count.times.map{queue.pop}
  end

  events.each_with_index do |event, i|
    insist { event["sequence"] } == i
  end
end # process

describe "inputs/redis", :redis => true do

  it "should read events from a list" do
    key = 10.times.collect { rand(10).to_s }.join("")
    event_count = 1000 + rand(50)
    # event_count = 100
    conf = <<-CONFIG
      input {
        redis {
          type => "blah"
          key => "#{key}"
          data_type => "list"
        }
      }
    CONFIG

    populate(key, event_count)
    process(conf, event_count)
  end

  it "should read events from a list using batch_count" do
    key = 10.times.collect { rand(10).to_s }.join("")
    event_count = 1000 + rand(50)
    conf = <<-CONFIG
      input {
        redis {
          type => "blah"
          key => "#{key}"
          data_type => "list"
          batch_count => #{rand(20)+1}
        }
      }
    CONFIG

    populate(key, event_count)
    process(conf, event_count)
  end
end
