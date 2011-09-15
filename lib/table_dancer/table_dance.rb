module TableDancer
  class TableDance < ActiveRecord::Base
    include DummyClasses
    
    validates_presence_of :source_table
    validate :source_table_exists
    
    before_create :record_destination_table_name
    before_create :set_initial_phase
    
    has_many :replays, :class_name => "TableDancer::TableDanceReplay"
    
    def self.setup(table_name)
      TableDance.create(
        :source_table => table_name
      )
    end
    
    def self.run!(table_name)
      dance = setup(table_name)
      dance.init!
      dance.copy!
      dance.replay!
      dance.cutover!
    end
    
    def init!
      within_table_lock do
        self.last_copy_id = find_last_copy_id
        install_triggers
        self.phase = 'copy'
        save!
      end
      self
    end
    
    def copy!
      raise(StandardError, "Cannot copy when not in copy phase") if phase != 'copy'
      
      source_class.find_each do |original|
        self.replays.create(
          :instruction => 1,
          :event_time => original.respond_to?(:created_at) ? original.created_at : self.created_at,
          :source_id => original.id
        )
      end
      
      update_attribute(:phase, 'replay')
      self
    end
    
    def replay!
      ActiveRecord::Base.logger.debug "Begin replay!"
      
      raise(StandardError, "Cannot replay when not in replay phase") if phase != 'replay'
      
      replay(20) # Replay down to only 20 max outstanding records
      update_attribute(:phase, 'cutover')
      self
    end
    
    def cutover!
      raise(StandardError, "Cannot cutover when not in cutover phase") if phase != 'cutover'

      replay(10) # Replay down to only 10 max outstanding records
      within_table_lock do
        replay(0)
        transaction do
          execute("ALTER TABLE `#{source_table}` RENAME TO `#{decommissioned_table}`")
          execute("ALTER TABLE `#{dest_table}` RENAME TO `#{source_table}`")
        end
        update_attribute(:phase, 'complete')
        self
      end
    end
      
    private

    def replay(target_size = 0)
      while batch = next_unperformed_batch
        batch.each do |replay|
          replay.perform!(source_table, dest_table, shared_columns)
        end
        break if batch.size <= target_size
      end
    end
    
    def shared_columns
      source_class.column_names & dest_class.column_names
    end
    
    def next_unperformed_batch
      replays.unperformed.all(:limit => 1000)
    end
    
    def replay_table
      'table_dance_replays'
    end
    
    def decommissioned_table
      "#{source_table}_decommissioned"
    end
    
    def record_destination_table_name
      self.dest_table = "#{source_table}_danced"
    end
    
    def set_initial_phase
      self.phase = 'init'
    end
    
    def lock_tables
      execute('SET autocommit=0;')
      table_locks = [source_table, dest_table, self.class.table_name].map {|t| "`#{t}` WRITE"}.join(', ')
      execute("LOCK TABLES #{table_locks};")
    end
    
    def unlock_tables
      execute('COMMIT;')
      execute('UNLOCK TABLES;')
      execute('SET autocommit=1;')
    end

    # Credit goes to https://github.com/freels/table_migrator/blob/master/lib/table_migrator/copy_engine.rb
    def within_table_lock
      begin
        lock_tables
        yield
      ensure
        unlock_tables
      end
    end
    
    def execute(sql)
      self.class.connection.execute(sql)
    end
    
    # A validation
    def source_table_exists
      if !ActiveRecord::Base.connection.table_exists?(source_table)
        self.errors.add(:source_table, "does not exist")
      end
    end
    
    def find_last_copy_id
      source_class.first(:order => "id DESC").try(:id)
    end
    
    def delete_triggers
      delete_trigger(Instructions::INSERT)
      delete_trigger(Instructions::UPDATE)
      delete_trigger(Instructions::DELETE)
    end
    
    def install_triggers
      delete_triggers
      install_trigger(Instructions::INSERT)
      install_trigger(Instructions::UPDATE)
      install_trigger(Instructions::DELETE)
    end
    
    def delete_trigger(trigger_type)
      name = trigger_type['name']
      execute("DROP TRIGGER IF EXISTS #{source_table}_after_#{name};")
    end

    def install_trigger(trigger_type)
      name = trigger_type['name']
      
      sql = "CREATE TRIGGER #{source_table}_after_#{name} AFTER #{name.upcase} ON `#{source_table}` " +
            "FOR EACH ROW INSERT INTO #{replay_table} (#{replay_columns}) VALUES (#{replay_values(trigger_type)});"
      execute(sql)
    end
    
    def replay_columns
      "`table_dance_id`, `instruction`, `event_time`, `source_id`"
    end

    def replay_values(instruction)
      "#{self.id}, #{instruction['id']}, CURRENT_TIMESTAMP, #{instruction['table_ref']}.id"
    end
  end
end