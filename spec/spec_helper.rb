require 'rspec'

require 'table_dancer'
require 'helpers'

require 'database_cleaner'

RSpec.configure do |config|
  config.before(:suite) do
    TableDancer.database_config_file = File.expand_path(File.join(File.dirname(__FILE__), 'config', 'database.yml'))
    TableDancer.log_file             = File.expand_path(File.join(File.dirname(__FILE__), '..', 'log', 'test.log'))
    TableDancer.establish_connection
    # TableDancer.verbose = true

    unless connection.table_exists?('table_dances')
      require 'config/test_migration'
      TestMigration.up
    end
    
    setup_foo_tables
    
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