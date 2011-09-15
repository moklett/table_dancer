module TableDancer
  module DummyClasses
    # Creates (or reuses) a new anonymous ActiveRecord::Base descendant that uses our source table
    def source_class
      @source_class ||= dummy_class_for(source_table)
    end
    
    # Creates (or reuses) a new anonymous ActiveRecord::Base descendant that uses our source table
    def destination_class
      @destination_class ||= dummy_class_for(dest_table)
    end
    alias_method :dest_class, :destination_class
    
    private
    
    def dummy_class_for(table)
      klass = Class.new(ActiveRecord::Base)
      klass.set_table_name table
      klass
    end
  end
end