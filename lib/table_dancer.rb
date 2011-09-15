require 'active_record'
require 'table_dancer/dummy_classes'
require 'table_dancer/instructions'
require 'table_dancer/speech'
require 'table_dancer/table_dance'
require 'table_dancer/table_dance_replay'

module TableDancer
  mattr_accessor :verbose
  mattr_writer   :rest_interval
  mattr_writer   :replay_iteration_threshold
  mattr_accessor :database_config_file
  mattr_writer   :database_config
  mattr_reader   :log_file
  
  def self.run!(table_name)
    TableDancer::TableDance.run!(table_name)
  end
  
  def self.setup(table_name)
    TableDancer::TableDance.setup(table_name)
  end
  
  def self.log(msg)
    logger.info(msg)
  end
  
  def self.logger
    @logger ||= ActiveRecord::Base.logger
  end
  
  def self.say(msg)
    if verbose?
      puts(msg)
    end
  end
  
  def self.resay(msg)
    if verbose?
      jump = "\r\e[0K" # That is return to beginning of line and use the
                       # ANSI clear command "\e" or "\003"
      $stdout.flush
      print "#{jump}#{msg}"
    end
  end
  
  def self.rest_interval
    @@rest_interval.to_i
  end
  
  def self.replay_iteration_threshold
    (@@replay_iteration_threshold || 20).to_i
  end
  
  def self.database_config
    YAML.load(File.open(database_config_file))
  end
  
  def self.establish_connection
    ActiveRecord::Base.establish_connection(database_config)
    attach_logger
  end
  
  def self.log_file=(file)
    @@log_file = file
    attach_logger
  end
  
  private
  
  def self.verbose?
    verbose == true
  end
  
  def self.attach_logger
    return if log_file.blank?
    ActiveRecord::Base.logger = Logger.new(log_file)
    ActiveRecord::Base.colorize_logging = false
  end
end