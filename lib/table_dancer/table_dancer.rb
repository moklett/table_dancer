module TableDancer
  mattr_accessor :verbose
  mattr_writer   :rest_interval
  mattr_writer   :outfile_rest_interval
  mattr_writer   :infile_rest_interval
  mattr_writer   :replay_iteration_threshold
  mattr_accessor :database_config_file
  mattr_writer   :database_config
  mattr_reader   :log_file
  mattr_writer   :batch_size
  mattr_writer   :outfile_record_limit
  mattr_writer   :outfile_dir
  
  def self.run!(table_name)
    TableDancer::TableDance.run!(table_name)
  end
  
  def self.setup(table_name)
    TableDancer::TableDance.setup(table_name)
  end
  
  def self.rest_interval
    @@rest_interval.to_i
  end
  
  def self.outfile_rest_interval
    @@outfile_rest_interval || rest_interval
  end
  
  def self.infile_rest_interval
    @@infile_rest_interval || rest_interval
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
  
  def self.outfile_record_limit
    (@@outfile_record_limit || 100000).to_i
  end
  
  def self.outfile_dir
    @@outfile_dir || File.join(ENV['HOME'], 'tmp')
  end
  
  private
  
  def self.attach_logger
    return if log_file.blank?
    ActiveRecord::Base.logger = Logger.new(log_file)
    ActiveRecord::Base.colorize_logging = false
  end
end