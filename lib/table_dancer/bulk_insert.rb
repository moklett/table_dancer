module TableDancer
  class BulkInsert
    attr_writer   :connection
    attr_accessor :table_name
    attr_accessor :columns
    
    DoublePerformError = Class.new(StandardError)
    
    def initialize(connection, table_name, columns = [])
      self.connection = connection
      self.table_name = table_name
      self.columns = columns
      @value_sets = []
    end

    def value_sets
      @value_sets
    end
    
    def push_values(set)
      @value_sets << set
    end
    
    def perform
      raise DoublePerformError if already_performed?
      
      unless value_sets.empty?
        sql = "INSERT INTO `#{table_name}` #{column_list} VALUES #{value_set_list}"
        execute(sql)
        @performed = true
      end
    end
    
    private
    
    def column_list
      "(#{columns.join(',')})"
    end
    
    # [['a','b'],['c','d']] => "('a','b'),('c','d')"
    def value_set_list
      value_sets.map do |set|
        "("+set.map {|value| @connection.quote(value) }.join(',')+")"
      end.join(',')
    end
    
    def execute(sql)
      @connection.execute(sql)
    end
    
    def already_performed?
      @performed == true
    end
  end
end