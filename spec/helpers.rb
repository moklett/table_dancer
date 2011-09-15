def connection
  ActiveRecord::Base.connection
end

def setup_foo_tables
  init_table('foos', {:string => 'title'})
  init_table('foos_danced', {:string => 'title'})
end

def init_table(table_name, columns = {})
  unless connection.table_exists?(table_name)
    connection.create_table(table_name) do |t|
      columns.each do |k,v|
        t.send(k, v)
      end
    end
  end
end
    