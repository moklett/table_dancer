module TableDancer
  class TableDance < ActiveRecord::Base
    include DummyClasses
    include Speech
    
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
      verify_phase!('init')
      announce_phase
      within_table_lock do
        record_last_copy_id
        install_triggers
        advance_phase
      end
      self
    end
    
    def copy!
      verify_phase!('copy')
      announce_phase
      copy_all_pre_trigger_records_to_replays
      advance_phase
      self
    end
    
    def replay!
      verify_phase!('replay')
      announce_phase
      replay(:down_to => TableDancer.replay_iteration_threshold) # Replay down to only N max outstanding records
      advance_phase
      self
    end
    
    def cutover!
      verify_phase!('cutover')
      announce_phase
      replay(:down_to => TableDancer.replay_iteration_threshold) # Replay down to only N max outstanding records
      within_table_lock do
        replay(:down_to => 0)
        say "Renaming tables"
        transaction do
          execute("ALTER TABLE `#{source_table}` RENAME TO `#{decommissioned_table}`")
          execute("ALTER TABLE `#{dest_table}` RENAME TO `#{source_table}`")
        end
        say "Rename complete"
        advance_phase
        self
      end
    end
    
    def copy_columns
      @copy_columns ||= source_class.column_names & dest_class.column_names
    end
    
    def validate_checksums
      say "Calculating checksums..."
      c1 = table_checksum(source_table)
      say "Checksum for #{source_table} is #{c1}"
      c2 = table_checksum("#{source_table}_decommissioned")
      say "Checksum for #{source_table} is #{c1}"
      if c1 == c2
        say "Checksums match! :)"
        return true
      else
        say "Checksums DO NOT match! :("
        return false
      end
    end
      
    private

    def announce_phase
      log "=========== BEGINNING #{phase.upcase} PHASE ================"
      say "Beginning #{phase} phase"
    end
    
    def verify_phase!(target_phase)
      raise(StandardError, "Cannot #{target_phase} when not in #{target_phase} phase") if phase != target_phase
    end

    def replay(options = {})
      options = {:down_to => 0}.merge!(options)
      TableDanceReplay.replay_each(self, options)
    end
    
    def shared_columns
      source_class.column_names & dest_class.column_names
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
      table_locks = [source_table, dest_table, self.class.table_name].map {|t| "`#{t}` WRITE"}.join(', ')
      execute('SET autocommit=0;')
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
      rescue => e
        raise e
      ensure
        unlock_tables
      end
    end
    
    def execute(sql)
      connection.execute(sql)
    end
    
    # A validation
    def source_table_exists
      if !connection.table_exists?(source_table)
        self.errors.add(:source_table, "does not exist")
      end
    end
    
    def find_last_copy_id
      record = source_class.first(:order => "id DESC")
      id = record ? record.id : 0
      id
    end
    
    def delete_triggers
      delete_trigger(Instructions::INSERT)
      delete_trigger(Instructions::UPDATE)
      delete_trigger(Instructions::DELETE)
    end
    
    def install_triggers
      say "Installing triggers", 1
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
    
    def record_last_copy_id
      say("Recording last copy ID", 1)
      self.last_copy_id = find_last_copy_id
      save
    end
    
    def advance_phase
      say "Completed #{phase} phase"
      phases = ['init', 'copy', 'replay', 'cutover', 'complete']
      next_phase_index = phases.index(phase)+1
      self.phase = phases[next_phase_index]
      save
    end
    
    def copy_all_pre_trigger_records_to_replays
      count = source_class.count(:conditions => "#{source_table}.id <= #{last_copy_id}")
      say "There are #{count} records to copy to the replays table", 1

      batch_num = 1
      index = 1
      
      source_class.find_in_batches(:conditions => "#{source_table}.id <= #{last_copy_id}", :batch_size => TableDancer.batch_size) do |batch|
        bulk_insert = BulkInsert.new(connection, TableDanceReplay.table_name, TableDanceReplay.column_names)
        
        batch_size = batch.size
        batch.each do |original|
          resay "Copying #{index} of #{count}", 2
          # This is a temporary object
          replay = TableDanceReplay.new(
            :table_dance_id => self.id,
            :instruction => 1,
            :event_time => original.respond_to?(:created_at) ? original.created_at : self.created_at,
            :source_id => original.id,
            :performed => false
          )
          # Get values in the same order as the column names on the bulk inserter
          value_set = bulk_insert.columns.map {|c| replay.send(c)}
          # Release temporary object
          bulk_insert.push_values(value_set)
          index = index+1
        end
        
        resay "Copying #{index} of #{count} (Bulk insert on batch #{batch_num})", 2
        bulk_insert.perform

        batch_num = batch_num+1

        sleep TableDancer.rest_interval if batch_size == TableDancer.batch_size
      end
      say "\n"
    end
    
    def table_checksum(table_name, max_id = 0)
      conditions = nil
      if max_id > 0
        conditions = "id <= #{max_id}"
      end
      checksum = ''
      dummy_class_for(table_name).find_each(:conditions => conditions) do |row|
        checksum = MD5.hexdigest("#{checksum}#{row.attributes}")
      end
      checksum
    end
  end
end