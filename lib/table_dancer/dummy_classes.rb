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
    
    def connection
      ActiveRecord::Base.connection
    end

    def dummy_class_for(table)
      @dummy_classes ||= {}
      @dummy_classes[table] ||= begin
        klass = Class.new(ActiveRecord::Base)
        klass.set_table_name table
        klass.set_inheritance_column nil
        klass
      end
    end
  end
end