#require 'gem_publisher'
#require "logstash/devutils/rake"

task :default do
  system('rake -T')
end

require 'jars/installer'
task :install_jars do
    ENV['JARS_HOME'] = Dir.pwd + "/vendor/jar-dependencies/runtime-jars"
    Jars::Installer.vendor_jars!
end
