# encoding: utf-8
require "logstash/outputs/base"
require "logstash/namespace"
require "java"
require "date"
require "logstash-output-ingestor_jars.rb"

class LogStash::Outputs::Ingestor < LogStash::Outputs::Base
  config_name "ingestor"

  default :codec, "json"

  config :schemaFile, :validate => :string, :required => false
  config :bootstrap_servers, :validate => :string, :required => true
  config :batch_size, :validate => :number, :default => 16384
  config :acks, :validate => ["0", "1", "all"], :default => "1"
  config :siem, :validate => :boolean, :required => true
  config :successTopic, :validate => :string, :required => true
  config :failureTopic, :validate => :string, :required => true

  public
  def register
    @producer = create_producer
  end

  def convert(data)
      if siem
          return data.to_i
      else
          return data
      end
  end

  def parsetimestamp(data)
      begin
          if data =~ /(.*)-(.*)/ or data =~/(.*)\/(.*)/
              return convert(DateTime.parse(data).strftime('%Q'))
          elsif data.length == 13
              return convert(data)
          elsif data.length == 10
              return convert(DateTime.parse(Time.at(data.to_i).to_s).strftime('%Q'))
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

    file = java.io.File.new(schema)
    schema = org.apache.avro.Schema::Parser.new().parse(file)
    extraInfo = java.util.HashMap.new
    record = org.apache.avro.generic.GenericData::Record.new(schema)
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
        time = convert(DateTime.now.strftime("%Q"))
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

    if !tool.nil?
        @producer.ingest(tool, record)
        @logger.debug("Ingestiong...\r\t#{record}")
    end
    rescue LogStash::ShutdownSignal
      @logger.info("Ingestor output got shutdown signal")
    rescue => e
      @logger.warn("Ingestor output threw exception, restarting\rError during processing: #{$!}\rBacktrace:\r\t#{e.backtrace.join("\r\t")}")
    end

  private
  def create_producer
    begin
        props = java.util.Properties.new
        kafka = org.apache.kafka.clients.producer.ProducerConfig

        props.put(kafka::BOOTSTRAP_SERVERS_CONFIG, bootstrap_servers)
        props.put(kafka::ACKS_CONFIG, acks)
        props.put(kafka::VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")
        props.put(kafka::KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")
        props.put(kafka::BATCH_SIZE_CONFIG, batch_size.to_s)

        base = Java::BrComProdubanOpenbusIngestor::OpenbusDataIngestion

        publisher = Java::BrComProdubanOpenbusPublisherKafka::KafkaAvroPublisher.new(props)

        if siem
            ingestion =  base::OpenbusDataIngestionBuilder.aIngestionConfiguration().withKafkaAvroPublisher(publisher).withSuccessTopic(successTopic).withFailureTopic(failureTopic).withNormalizer(Java::BrComProdubanOpenbusRulesNormalizerServices::LogstashNormalizer.new).build()
        else
            ingestion = Java::BrComProdubanOpenbusIngestor::OpenbusDataIngestion.new(publisher, successTopic, failureTopic)
        end
    rescue => e
      @logger.error("Unable to instantiate insgestor #{$!}\rBacktrace:\r\t#{e.backtrace.join("\r\t")}")
      raise e
    end
  end
end
