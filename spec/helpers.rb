def config_file
  File.expand_path(File.join(File.dirname(__FILE__), 'config', 'database.yml'))
end

def log_file
  File.expand_path(File.join(File.dirname(__FILE__), '..', 'log', 'test.log'))
end

def connection
  ActiveRecord::Base.connection
end

def init_table(table_name)
  connection.create_table(table_name) {|t| t.string :title } unless connection.table_exists?(table_name)
end
    