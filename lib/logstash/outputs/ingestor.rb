# encoding: utf-8
require "logstash/outputs/base"
require "logstash/namespace"
require "java"
require "avro"
require "date"
require "logstash-output-ingestor_jars.rb"

class LogStash::Outputs::Ingestor < LogStash::Outputs::Base
  config_name "ingestor"
  java_import org.apache.avro.generic.GenericData
  java_import org.apache.avro.Schema

  default :codec, "json"

  config :schemaFile, :validate => :string
  config :brokerList, :validate => :string, :required => true
  config :batchNumMessages, :validate => :string
  config :requiredAcks, :validate => :boolean
  config :isAsync, :validate => :boolean
  config :successTopic, :validate => :string, :required => true
  config :failureTopic, :validate => :string, :required => true

  public
  def register
    @producer = create_producer
  end

  def parsetimestamp(data)
      begin
          if data =~ /(.*)-(.*)/ or data =~/(.*)\/(.*)/
              return DateTime.parse(data).strftime('%Q')
          elsif data.length == 13
              return data
          elsif data.length == 10
              return DateTime.parse(Time.at(data.to_i).to_s).strftime('%Q')
          end
      rescue => e
          @logger.error("Error during processing: #{$!}\rBacktrace:\r\t#{e.backtrace.join("\r\t")}")
          raise e
      rescue
      end
  end

  def receive(event)
    if event == LogStash::SHUTDOWN
      return
    end

    # check schemaFile
    sf = schemaFile if sf.nil? || sf.empty?
    if sf.nil?
      schema = event["schemaFile"]
    else
      schema = schemaFile
    end

    file = Java::JavaIo::File.new(schema)
    schema = Schema::Parser.new().parse(file)
    extraInfo = Java::JavaUtil::HashMap.new
    record = GenericData::Record.new(schema)
    tool = event["tool"]
    fields = schema.getFields

    # fill information
    fields.each do |field|
        fieldName = field.name
        record.put(fieldName, event[fieldName])
    end

    # check timestamp
    ts = event["timestamp"] if ts.nil? || ts.empty?
    if ts.nil?
        time = DateTime.now.strftime("%Q")
    else
        time = parsetimestamp(ts)
    end

    record.put("timestamp", time)

    # check extraInfo information
    ei = event["extraInfo"] if ei.nil? || ei.empty?
    if !ei.nil?
        ei.each do |key,value|
            extraInfo.put(key, value)
        end
    end

    record.put("extraInfo", extraInfo)

    @producer.ingest(tool, record)
    @logger.debug(record)
    rescue LogStash::ShutdownSignal
      @logger.info("Ingestor output got shutdown signal")
    rescue => e
      @logger.warn("Ingestor output threw exception, restarting\rError during processing: #{$!}\rBacktrace:\r\t#{e.backtrace.join("\r\t")}")
    end

  private
  def create_producer
    begin
      publisher = Java::BrComOpenbusPublisherKafka::KafkaAvroPublisher.new(brokerList,requiredAcks,isAsync,batchNumMessages)
      ingestion = Java::BrComProdubanOpenbusIngestor::OpenbusDataIngestion.new(publisher, successTopic, failureTopic)
    rescue => e
      @logger.error("Unable to instantiate insgestor #{$!}\rBacktrace:\r\t#{e.backtrace.join("\r\t")}")
      raise e
    end
  end
end
