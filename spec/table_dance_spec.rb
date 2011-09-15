require 'spec_helper'

module TableDancer
  describe TableDance, ".setup" do
    it "creates a new tracking entry" do
      lambda { TableDance.setup('foos') }.should change { TableDance.count }.by(1)
    end
  
    it "requires a table name as input" do
      lambda { TableDance.setup() }.should raise_error(ArgumentError)
    end
  
    it "requires the source_table to exist" do
      dance = TableDance.setup('non_existant')
      dance.errors.on(:source_table).should == "does not exist"
    end
  
    it "requires the dest_table to exist" do
      dance = TableDance.setup('non_existant')
      dance.errors.on(:source_table).should == "does not exist"
    end
  
    it "records the source table name in the tracking entry" do
      dance = TableDance.setup('foos')
      dance.source_table.should == 'foos'
    end
  
    it "calculates and stores the destination table name by appending '_danced' to the source table" do
      dance = TableDance.setup('foos')
      dance.dest_table.should == 'foos_danced'
    end
  end
  
  describe TableDance, ".setup", "creates an instance that" do
    let(:dance) { TableDance.setup('foos') }

    it "begins in 'init' phase" do
      dance.phase.should == 'init'
    end
  end
  
  describe TableDance, "#init!" do
    let(:dance) { TableDance.setup('foos') }
    
    it "locks and unlocks the source table" do
      dance.should_receive(:lock_tables)
      dance.should_receive(:save!)
      dance.should_receive(:unlock_tables)
      dance.init!
    end
    
    it "records the final existing id of the source table in last_copy_id" do
      num = 2
      num.times { dance.source_class.create }

      dance.last_copy_id.should be_blank
      dance.init!
      dance.last_copy_id.should == num
    end
    
    it "installs the triggers" do
      dance.should_receive(:install_triggers)
      dance.init!
    end
    
    it "installs the correct triggers" do
      dance.init!
      result = connection.execute("SHOW TRIGGERS;")
      hashes = result.all_hashes

      result.num_rows.should == 3
      hashes.find {|r| r['Event'] == 'INSERT'}['Statement'].should == "INSERT INTO table_dance_replays (`table_dance_id`, `instruction`, `event_time`, `source_id`) VALUES (#{dance.id}, 1, CURRENT_TIMESTAMP, NEW.id)"
      hashes.find {|r| r['Event'] == 'UPDATE'}['Statement'].should == "INSERT INTO table_dance_replays (`table_dance_id`, `instruction`, `event_time`, `source_id`) VALUES (#{dance.id}, 2, CURRENT_TIMESTAMP, NEW.id)"
      hashes.find {|r| r['Event'] == 'DELETE'}['Statement'].should == "INSERT INTO table_dance_replays (`table_dance_id`, `instruction`, `event_time`, `source_id`) VALUES (#{dance.id}, 3, CURRENT_TIMESTAMP, OLD.id)"
      result.free
    end
    
    it "moves the phase to 'copy'" do
      dance.phase.should == 'init'
      dance.init!
      dance.reload
      dance.phase.should == 'copy'
    end
    
    it "returns itself" do
      dance.init!.should == dance
    end
  end
  
  describe TableDance, "#copy!" do
    before do
      # Create 3 existing rows
      3.times { dance.source_class.create }
    end
    
    let(:dance) { TableDance.setup('foos').init! }
    
    it "raises an error if not in 'copy' phase" do
      dance.update_attribute(:phase, 'init')
      lambda { dance.copy! }.should raise_error(StandardError, 'Cannot copy when not in copy phase')
    end
    
    it "creates an 'insert' replay entry for each existing row" do
      lambda { dance.copy! }.should change { dance.replays.inserts.count }.by(3)
    end
    
    it "moves the phase to 'replay'" do
      dance.copy!
      dance.reload
      dance.phase.should == 'replay'
    end
    
    it "returns itself" do
      dance.copy!.should == dance
    end
    
  end

  describe TableDance, "#replay!" do
    let(:dance) do
      d = TableDance.setup('foos')
      d.send(:delete_triggers)

      # Start with 2 records
      2.times { d.source_class.create! }
      
      d.init!
      
      # Modify one of the recores and delete the other
      d.source_class.first.update_attributes!(:title => "changed!") # Replay 1
      d.source_class.last.destroy                                   # Replay 2
      
      d.copy!                                                       # Replay 3
      d
    end
    
    it "raises an error if not in 'replay' phase" do
      dance.update_attribute(:phase, 'init')
      lambda { dance.replay! }.should raise_error(StandardError, 'Cannot replay when not in replay phase')
    end

    it "orders the replays by instruction type then the original event time" do
      replays = dance.replays

      replays[0]['id'].should == 3
      replays[0]['source_id'].should == 1
      replays[0]['instruction'].should == Instructions::INSERT['id']
      
      replays[1]['id'].should == 1
      replays[1]['source_id'].should == 1
      replays[1]['instruction'].should == Instructions::UPDATE['id']
      
      replays[2]['id'].should == 2
      replays[2]['source_id'].should == 2
      replays[2]['instruction'].should == Instructions::DELETE['id']
    end
    
    it "replays each unperformed instruction" do
      dance.replays.unperformed.count.should == 3
      dance.replay!
      dance.reload
      dance.replays.unperformed.count.should == 0
    end
    
    it "moves the phase to 'cutover'" do
      dance.replay!
      dance.reload
      dance.phase.should == 'cutover'
    end
    
    it "returns itself" do
      dance.replay!.should == dance
    end
    
  end
  
  describe TableDance, "#cutover!" do
    before do
      init_table('foos')
      init_table('foos_danced')
      connection.drop_table('foos_decommissioned') if connection.table_exists?('foos_decommissioned')
    end
    
    let(:dance) { d = TableDance.setup('foos').init!.copy!.replay! }

    it "raises an error if not in 'cutover' phase" do
      dance.update_attribute(:phase, 'init')
      lambda { dance.cutover! }.should raise_error(StandardError, 'Cannot cutover when not in cutover phase')
    end

    it "locks and unlocks the source table" do
      dance.should_receive(:lock_tables)
      dance.should_receive(:unlock_tables)
      dance.cutover!
    end
    
    it "should rename the tables" do
      dance.cutover!
      connection.table_exists?('foos_decommissioned').should be_true
      connection.table_exists?('foos').should be_true
      connection.table_exists?('foos_danced').should be_false
    end

    it "moves the phase to 'complete'" do
      dance.cutover!
      dance.reload
      dance.phase.should == 'complete'
    end
    
    it "returns itself" do
      dance.cutover!.should == dance
    end
    
  end
end