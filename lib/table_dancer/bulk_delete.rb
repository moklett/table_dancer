module TableDancer
  class BulkDelete
    attr_writer   :connection
    attr_accessor :table_name
    
    DoublePerformError = Class.new(StandardError)
    
    def initialize(connection, table_name)
      self.connection = connection
      self.table_name = table_name
      @record_ids = []
      @performed = false
    end

    def record_ids
      @record_ids
    end
    
    def push_id(id)
      @record_ids << id
    end
    
    def perform
      raise DoublePerformError if already_performed?
      
      unless record_ids.empty?
        sql = "DELETE FROM `#{table_name}` WHERE id IN (#{id_list})"
        execute(sql)
        @performed = true
      end
    end
    
    private
    
    def id_list
      "#{record_ids.join(',')}"
    end
    
    def execute(sql)
      @connection.execute(sql)
    end
    
    def already_performed?
      @performed == true
    end
  end
end