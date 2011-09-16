module TableDancer
  mattr_accessor :verbose
  mattr_writer   :rest_interval
  mattr_writer   :replay_iteration_threshold
  mattr_accessor :database_config_file
  mattr_writer   :database_config
  mattr_reader   :log_file
  mattr_writer   :batch_size
  
  def self.run!(table_name)
    TableDancer::TableDance.run!(table_name)
  end
  
  def self.setup(table_name)
    TableDancer::TableDance.setup(table_name)
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
    return ActiveRecord::Base.connection
  end
  
  def self.log_file=(file)
    @@log_file = file
    attach_logger
  end
  
  def self.batch_size
    (@@batch_size || 1000).to_i
  end
  
  private
  
  def self.attach_logger
    return if log_file.blank?
    ActiveRecord::Base.logger = Logger.new(log_file)
    ActiveRecord::Base.colorize_logging = false
  end
end