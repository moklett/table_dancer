module TableDancer
  class TableDanceReplay < ActiveRecord::Base
    include DummyClasses
    
    attr_accessor :source_table
    attr_accessor :dest_table
    attr_accessor :shared_columns
    
    belongs_to :dance, :class_name => 'TableDancer::TableDance', :foreign_key => :table_dance_id
    
    default_scope :order => "instruction ASC, event_time ASC"
    
    named_scope :inserts, :conditions => {:instruction => Instructions::INSERT['id']}
    named_scope :updates, :conditions => {:instruction => Instructions::UPDATE['id']}
    named_scope :deletes, :conditions => {:instruction => Instructions::DELETE['id']}
    
    named_scope :unperformed, :conditions => {:performed => false}
    
    def perform!(source_table = nil, dest_table = nil, columns = [])
      return if performed?
      
      self.source_table = source_table || dance.source_table
      self.dest_table   = dest_table   || dance.dest_table

      self.send("replay_#{instruction_name}")

      update_attribute(:performed, true)
    end
    
    private
    
    def instruction_name
      instructions_by_id[instruction.to_i]['name']
    end
    
    def instructions_by_id
      [Instructions::INSERT, Instructions::UPDATE, Instructions::DELETE].index_by {|i| i['id'] }
    end
    
    def replay_insert
      original = source_class.find(source_id)
      return if original.nil?
      dest = dest_class.new
      save_obj_with_attributes(dest, original.attributes)
    end

    def replay_update
      original = source_class.find(source_id)
      return if original.nil?
      dest = dest_class.find(source_id)
      if dest.nil?
        replay_insert
      else
        save_obj_with_attributes(dest, original.attributes)
      end
    end
    
    def replay_delete
      dest_class.delete(source_id)
    end
    
    def save_obj_with_attributes(obj, attributes)
      attributes.each do |attr, val|
        Rails.logger.debug("======== #{obj.inspect}")
        Rails.logger.debug("======== checking #{attr}")
        if obj.respond_to?("#{attr}=")
          Rails.logger.debug("======== writing #{attr} with #{val}")
          obj.send("#{attr}=", val)
        end
      end
      begin
        obj.save(false)
      rescue ActiveRecord::StatementInvalid => e
        if e.message =~ /^Mysql::Error: Duplicate entry '\d+' for key 'PRIMARY'/
          true # we're okay!
        else
          raise e
        end
      end
    end
  end
end