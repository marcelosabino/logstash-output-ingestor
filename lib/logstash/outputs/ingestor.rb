# encoding: utf-8
require 'logstash/outputs/base'
require 'logstash/namespace'
require 'java'
require 'avro'
require 'logstash-output-ingestor_jars.rb'

class LogStash::Outputs::Ingestor < LogStash::Outputs::Base
  config_name 'ingestor'
  java_import org.apache.avro.generic.GenericData
  java_import org.apache.avro.Schema

  default :codec, 'json'

  config :schemaFile, :validate => :string, :required => true
  config :brokerList, :validate => :string, :required => true
  config :batchNumMessages, :validate => :string
  config :requiredAcks, :validate => :boolean
  config :isAsync, :validate => :boolean
  config :successTopic, :validate => :string, :required => true
  config :failureTopic, :validate => :string, :required => true
  config :resolvedTool, :validate => :string, :required => true

  public
  def register
    @producer = create_producer
  end # def register

  def receive(event)
    if event == LogStash::SHUTDOWN
      return
    end

    file = Java::JavaIo::File.new(schemaFile)
    schema = Schema::Parser.new().parse(file)
    extraInfo = Java::JavaUtil::HashMap.new
    record = GenericData::Record.new(schema)
    fields = schema.getFields
    fields.each do |field|
        value = field.name
        if value != "extraInfo"
            record.put(value, event[value])
        else
            record.put("extraInfo", event["extraInfo"])
        end
    end


#    record = GenericData::Record.new(schema)
#    record.put("hostname", event["hostname"])
#    record.put("metric", event["metric"])
#    record.put("value", event["value"])
#    record.put("timestamp", event["timestamp"])
#    record.put("extraInfo", extraInfo)

#    @producer.ingest(resolvedTool, Java::BrComProdubanOpenbusRulesBaseUtils::AvroUtils.changeUtf8ToString(record))
    @producer.ingest(resolvedTool, record)
    @logger.info(record)
    rescue LogStash::ShutdownSignal
      @logger.info('Ingestor output got shutdown signal')
    rescue => e
      @logger.warn('Ingestor output threw exception, restarting',
                   :exception => e)
    end # def receive

  private
  def create_producer
    begin
      publisher = Java::BrComOpenbusPublisherKafka::KafkaAvroPublisher.new(brokerList,requiredAcks,isAsync,batchNumMessages)
      resolvedTool = resolvedTool
      ingestion = Java::BrComProdubanOpenbusIngestor::OpenbusDataIngestion.new(publisher, successTopic, failureTopic)
    rescue => e
      logger.error("Unable to Ingestor creation.", :exception => e)
      raise e
    end
  end
end
