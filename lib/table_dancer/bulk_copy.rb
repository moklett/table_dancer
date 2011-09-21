module TableDancer
  class BulkCopy
    attr_writer   :connection
    attr_accessor :source_table
    attr_accessor :dest_table
    attr_accessor :columns
    
    DoublePerformError = Class.new(StandardError)
    
    def initialize(connection, source_table, dest_table, columns = [])
      self.connection = connection
      self.source_table = source_table
      self.dest_table = dest_table
      self.columns = columns
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
      
      unless record_ids.blank?
        sql = "REPLACE INTO `#{dest_table}` (#{column_list}) SELECT #{column_list} FROM `#{source_table}` WHERE id IN (#{id_list})"
        execute(sql)
        @performed = true
      end
    end
    
    private
    
    def column_list
      "#{columns.join(',')}"
    end
    
    def id_list
      "#{record_ids.uniq.join(',')}"
    end
    
    def execute(sql)
      @connection.execute(sql)
    end
    
    def already_performed?
      @performed == true
    end
  end
end