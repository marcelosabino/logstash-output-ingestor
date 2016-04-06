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

  config :schemaFile, :validate => :string, :required => true
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
      rescue TypeError => e
          logger.error("Exception:", :exception => e)
          raise e
      rescue
      end
  end

  def receive(event)
    if event == LogStash::SHUTDOWN
      return
    end

    file = Java::JavaIo::File.new(schemaFile)
    schema = Schema::Parser.new().parse(file)
    extraInfo = Java::JavaUtil::HashMap.new
    record = GenericData::Record.new(schema)
    tool = event["tool"]
    fields = schema.getFields
    fields.each do |field|
        fieldName = field.name
        record.put(fieldName, event[fieldName])
    end

    ts = event["timestamp"] if ts.nil? || ts.empty?
    ei = event["extraInfo"] if ei.nil? || ei.empty? 

    if ts.nil?
        time = DateTime.now.strftime("%Q")
    else
        time = parsetimestamp(ts)
    end

    record.put("timestamp", time)

    if !ei.nil?
        ei.each do |key,value|
            extraInfo.put(key, value)
        end
    end

    record.put("extraInfo", extraInfo)

    @producer.ingest(tool, record)
    @logger.info(record)
    rescue LogStash::ShutdownSignal
      @logger.info("Ingestor output got shutdown signal")
    rescue => e
      @logger.warn("Ingestor output threw exception, restarting",
                   :exception => e)
    end

  private
  def create_producer
    begin
      publisher = Java::BrComOpenbusPublisherKafka::KafkaAvroPublisher.new(brokerList,requiredAcks,isAsync,batchNumMessages)
      ingestion = Java::BrComProdubanOpenbusIngestor::OpenbusDataIngestion.new(publisher, successTopic, failureTopic)
    rescue => e
      logger.error("Unable to instantiate Insgestor ", :exception => e)
      raise e
    end
  end
end
