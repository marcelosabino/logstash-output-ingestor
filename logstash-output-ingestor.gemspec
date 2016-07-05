# coding: utf-8
Gem::Specification.new do |s|
  s.name = 'logstash-output-ingestor'
  s.version = "2.0.0"
  s.licenses = ["Apache-2.0"]
  s.summary = "Output events to ingestor. This uses Ingestor API to write messages to Kafka."
  s.description = "This gem is a logstash plugin required to be installed on top of the Logstash core pipeline using $LS_HOME/bin/plugin install gemname. This gem is not a stand-alone program"
  s.authors = ["Marcelo Sabino"]
  s.email = "marcelo.sabino@gmail.com"
  s.homepage = "http://www.elastic.co/guide/en/logstash/current/index.html"
  s.require_paths = ["lib"]

  # Files
  s.files = Dir['lib/**/*','spec/**/*','vendor/**/*','*.gemspec','*.md','CONTRIBUTORS','Gemfile','LICENSE','NOTICE.TXT']

   # Tests
  s.test_files = s.files.grep(%r{^(test|spec|features)/})

  # Special flag to let us know this is actually a logstash plugin
  s.metadata = { "logstash_plugin" => "true", "group" => "output" }
  s.requirements << "jar 'org.apache.kafka:kafka-clients', '0.8.2.1'"
  s.requirements << "jar 'org.slf4j:slf4j-api', '1.6.4'"
  s.requirements << "jar 'org.slf4j:slf4j-simple', '1.6.4'"
  s.requirements << "jar 'br.com.produban:openbus.ingestor', '0.0.1-SNAPSHOT'"

  # Gem dependencies
  s.add_development_dependency "jar-dependencies", "~> 0.3.2"
  s.add_development_dependency "logstash-devutils", "~> 0.0.10"

  s.add_runtime_dependency "logstash-core", ">= 2.0.0", "< 3.0.0"
  s.add_runtime_dependency "logstash-codec-json", "~> 2.0", ">= 2.0.2"
end

