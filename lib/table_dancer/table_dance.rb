module TableDancer
  class TableDance < ActiveRecord::Base
    include DummyClasses
    include Speech
    
    validates_presence_of :source_table
    validate :source_table_exists
    
    before_create :record_destination_table_name
    before_create :set_initial_phase
    
    has_many :replays, :class_name => "TableDancer::TableDanceReplay"
    
    attr_writer :options
    
    def self.setup(table_name, options = {})
      d = TableDance.create(
        :source_table => table_name
      )
      d.options = options
      d
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
        install_triggers if install_triggers?
        advance_phase
      end
      self
    end
    
    def copy!
      verify_phase!('copy')
      announce_phase
      copy_all_pre_trigger_records_to_dest_table
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
      say "Checksum for decommissioned table is #{c2}"
      if c1 == c2
        say "Checksums match! :)"
        return true
      else
        say "Checksums DO NOT match! :("
        return false
      end
    end

    def options
      @options || {}
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
      table_locks = [source_table, dest_table, self.class.table_name, self.replays.table_name].map {|t| "`#{t}` WRITE"}.join(', ')
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
    
    def copy_all_pre_trigger_records_to_dest_table
      count = source_class.count(:conditions => "#{source_table}.id <= #{last_copy_id}")
      say "Copying records to destination table", 1

      select_into_outfiles
      load_data_from_outfiles
    end
    
    # Dropping to command line for now
    def select_into_outfiles
      return if runid_given?
      
      say "Selecting data into outfiles..."
      
      start_id = 0
      batch = 0
      
      while start_id < last_copy_id
        batch = batch+1
        
        filename = outfile_name(batch)

        max_id = [start_id+TableDancer.outfile_record_limit, last_copy_id].min
        
        nc = '\\\\N'
        sedcmd = %{sed -e 's/^NULL$/#{nc}/g' | sed -e 's/^NULL\t/#{nc}\t/g' | sed -e 's/\tNULL$/\t#{nc}/g' | sed -e 's/\tNULL\t/\t#{nc}\t/g' | sed -e 's/\tNULL\t/\t#{nc}\t/g'}

        command = %Q{#{mysql} --skip-column-names -e "SELECT #{copy_columns.join(',')} FROM #{source_table} } +
                  %Q{WHERE id > #{start_id} AND id <= #{max_id} ORDER BY id" } +
                  %Q{| #{sedcmd} } +
                  %Q{> #{filename}}
        say "Writing #{filename}", 1
        system(command)

        start_id = start_id + TableDancer.outfile_record_limit
        sleep TableDancer.outfile_rest_interval
      end
      
      say "Done"
    end
    
    def load_data_from_outfiles
      say "Loading data from outfiles..."
      outfiles.each do |file|
        abort_if_lockfile_exists(file)
        write_lockfile(file)

        command = %Q{#{mysql} --local-infile -e "set foreign_key_checks=0; set sql_log_bin=0; set unique_checks=0; LOAD DATA LOCAL INFILE '#{file}' INTO TABLE #{dest_table} (#{copy_columns.join(',')});"}
        say "Reading #{file}", 1
        system(command)
        say "Reading complete. Removing #{file}", 1

        File.unlink(file)
        sleep TableDancer.infile_rest_interval
      end
      say "Done"
    end
    
    def mysql
      user = TableDancer.database_config['username']
      host = TableDancer.database_config['host']
      port = TableDancer.database_config['port']
      pass = TableDancer.database_config['password']
      db   = TableDancer.database_config['database']
      
      options = []
      
      options << "-u #{user}" if user
      options << "-h #{host}" if host
      options << "--port #{port}" if port
      options << "-p#{pass}" if pass
      options << "-D #{db}" if db
      
      "mysql #{options.join(' ')}"
    end
    
    def outfile_name(batch)
      File.join(outfile_dir, "#{source_table}_#{'%06d' % batch.to_i}.txt")
    end
    
    def outfile_dir
      FileUtils.mkdir_p(File.join(TableDancer.outfile_dir, runid.to_s))
    end
    
    def runid
      return @runid if defined?(@runid)
      @runid = options[:runid] || Time.now.to_i
    end
    
    def outfiles
      Dir.glob(File.join(outfile_dir, "#{source_table}_*.txt")).sort
    end
    
    def abort_if_lockfile_exists(outfile)
      lockfile = lockfile_name_for(outfile)
      if File.exist?(lockfile)
        raise StandardError, "File locked! #{lockfile}: #{File.read(lockfile)}"
      end
    end
    
    def write_lockfile(outfile)
      lockfile = lockfile_name_for(outfile)
      File.open(lockfile, "w") do |file|
        file.write "host:#{Socket.gethostname} pid:#{Process.pid}"
      end
    end
    
    def lockfile_name_for(outfile)
      outfile.gsub(/\.txt$/, ".lock")
    end
    
    def table_checksum(table_name, max_id = 0)
      conditions = nil
      if max_id > 0
        conditions = "id <= #{max_id}"
      end
      checksum = ''
      dummy_class_for(table_name).find_each(:conditions => conditions) do |row|
        checksum = MD5.hexdigest("#{checksum}#{row.attributes.select{|k,v| copy_columns.include?(k) }}")
      end
      checksum
    end
    
    def install_triggers?
      options[:install_triggers].nil? || options[:install_triggers]
    end
    
    def runid_given?
      not options[:runid].nil?
    end
  end
end