module TableDancer
  class TableDanceReplay < ActiveRecord::Base
    include DummyClasses
    include Speech
    
    belongs_to :dance, :class_name => 'TableDancer::TableDance', :foreign_key => :table_dance_id
    
    default_scope :order => "instruction ASC, event_time ASC"
    
    delegate :source_table, :to => :dance
    delegate :dest_table, :to => :dance
    delegate :copy_columns, :to => :dance

    named_scope :inserts, :conditions => {:instruction => Instructions::INSERT['id']}
    named_scope :updates, :conditions => {:instruction => Instructions::UPDATE['id']}
    named_scope :deletes, :conditions => {:instruction => Instructions::DELETE['id']}
    
    named_scope :unperformed, :conditions => {:performed => false}
    named_scope :for_dance, lambda { |dance_or_id| {:conditions => {:table_dance_id => dance_or_id.to_param.to_i}} }
    named_scope :batched, :limit => 1000
    
    def self.replay_each(dance, options = {})
      options = {:down_to => 0}.merge!(options)
      say "Beginning a replay run down to at least #{options[:down_to]} unperformed events"
      while batch = next_unperformed_batch(dance)
        count = batch.size
        index = 1
        say "Replaying on a batch of #{count} replay records"
        batch.each do |replay|
          resay "Replaying #{index} of #{count}"
          replay.dance = dance # prevent a lookup
          replay.perform!
          index = index+1
        end
        say "\nCompleted batch"
        if batch.size <= options[:down_to]
          say "Batch size was below the target of #{options[:down_to]}, exiting replay"
          break 
        end
      end
    end
    
    def perform!
      return if performed?
      transaction do
        self.send("replay_#{instruction_name}")
        update_attribute(:performed, true)
      end
    end
    
    private
    
    def replay_insert
      TableDancer.log "=== Replaying Insert of #{source_table} #{source_id}"
      sql = "REPLACE INTO #{dest_table} (#{copy_column_list}) SELECT #{copy_column_list} FROM #{source_table} WHERE id=#{source_id}"
      connection.execute(sql)
    end
    
    def replay_update
      replay_insert
    end
    
    def replay_delete
      connection.delete("DELETE FROM #{dest_table} WHERE id=#{source_id}")
    end
    
    def copy_column_list
      copy_columns.join(',')
    end
    
    def copy_values(hash)
      copy_columns.map { |col| hash[col] }
    end
    
    def copy_values_list(hash)
      copy_values(hash).join(',')
    end
    
    def self.next_unperformed_batch(dance)
      for_dance(dance).unperformed.batched.all
    end
    
    def instruction_name
      instructions_by_id[instruction.to_i]['name']
    end
    
    def instructions_by_id
      [Instructions::INSERT, Instructions::UPDATE, Instructions::DELETE].index_by {|i| i['id'] }
    end
  end
end