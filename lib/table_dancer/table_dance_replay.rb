module TableDancer
  class TableDanceReplay < ActiveRecord::Base
    include DummyClasses
    include Speech
    
    belongs_to :dance, :class_name => 'TableDancer::TableDance', :foreign_key => :table_dance_id
    
    delegate :source_table, :to => :dance
    delegate :dest_table, :to => :dance
    delegate :copy_columns, :to => :dance

    named_scope :inserts, :conditions => {:instruction => Instructions::INSERT['id']}
    named_scope :updates, :conditions => {:instruction => Instructions::UPDATE['id']}
    named_scope :deletes, :conditions => {:instruction => Instructions::DELETE['id']}
    
    named_scope :unperformed, :conditions => {:performed => false}
    named_scope :for_dance, lambda { |dance_or_id| {:conditions => {:table_dance_id => dance_or_id.to_param.to_i}} }
    named_scope :batched, lambda { {:limit => TableDancer.batch_size, :order => "event_time ASC"} }
    
    def self.replay_each(dance, options = {})
      options = {:down_to => 0}.merge!(options)
      say "Beginning a replay run down to at least #{options[:down_to]} unperformed events"

      count = for_dance(dance).unperformed.count
      batch_num = 1
      index = 1
      
      while batch = next_unperformed_batch(dance)
        resay "Replaying #{index} of #{count} (Batch #{batch_num})", 1
        
        reset_bulk_receptors(dance)
        batch.each do |replay|
          replay.stage!
        end
        batch_num = batch_num +1
        index = index+batch.size

        transaction do
          bulk_copy.perform
          bulk_delete.perform
          update_all({:performed => true}, {:id => batch.collect(&:id)})
        end

        if batch.size <= options[:down_to]
          say "Batch size was below the target of #{options[:down_to]}, exiting replay", 1
          break 
        else
          sleep TableDancer.rest_interval
        end
      end
      say "\n"
    end
    
    def stage!
      case instruction
      when Instructions::INSERT['id'], Instructions::UPDATE['id']
        bulk_copy.push_id(source_id)
      when Instructions::DELETE['id']
        bulk_delete.push_id(source_id)
      end
    end
    
    private
    
    def self.next_unperformed_batch(dance)
      for_dance(dance).unperformed.batched.all
    end
    
    def self.reset_bulk_receptors(dance)
      @bulk_copy = BulkCopy.new(connection, dance.source_table, dance.dest_table, dance.copy_columns)
      @bulk_delete = BulkDelete.new(connection, dance.dest_table)
    end
    
    def self.bulk_copy
      @bulk_copy
    end
    
    def bulk_copy
      self.class.bulk_copy
    end
    
    def self.bulk_delete
      @bulk_delete
    end
    
    def bulk_delete
      self.class.bulk_delete
    end
  end
end