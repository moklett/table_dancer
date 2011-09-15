require 'rspec'

require 'table_dancer'
require 'helpers'

require 'database_cleaner'

RSpec.configure do |config|
  config.before(:suite) do
    ActiveRecord::Base.establish_connection(YAML.load(File.open(config_file)))
    ActiveRecord::Base.colorize_logging = false
    ActiveRecord::Base.logger = Logger.new(log_file)

    unless connection.table_exists?('table_dances')
      require 'config/test_migration'
      TestMigration.up
    end
    
    init_table('foos')
    init_table('foos_danced')
    
    DatabaseCleaner.strategy = :truncation
    DatabaseCleaner.clean_with(:truncation)
  end
  
  config.after(:suite) do
    DatabaseCleaner.clean_with(:truncation)
  end

  config.before(:each) do
    DatabaseCleaner.start
  end

  config.after(:each) do
    DatabaseCleaner.clean
  end

end