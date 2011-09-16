require 'spec_helper'

module TableDancer
  describe BulkCopy, "new instance" do
    let(:source_table) { 'account_transactions' }
    let(:dest_table) { 'account_transactions_danced' }
    let(:columns) { ['one', 'two', 'three'] }
    
    it "has a private database connection given by the 1st argument" do
      bc = BulkCopy.new(connection, source_table, dest_table)
      bc.instance_variable_get(:@connection).should == connection
    end
    
    it "has a source_table that corresponds with the 2nd argument" do
      bc = BulkCopy.new(connection, source_table, dest_table)
      bc.source_table.should == source_table
    end

    it "has a dest_table that corresponds with the 3rd argument" do
      bc = BulkCopy.new(connection, source_table, dest_table)
      bc.dest_table.should == dest_table
    end
    
    it "has a list of columns that correspond with the 4th argument" do
      bc = BulkCopy.new(connection, source_table, dest_table, columns)
      bc.columns.should == columns
    end
    
    it "allows you to set the columns through an accessor" do
      bc = BulkCopy.new(connection, source_table, dest_table)
      bc.columns = columns
      bc.columns.should == columns
    end
    
    it "holds an empty array of columns if they are not set" do
      bc = BulkCopy.new(connection, source_table, dest_table)
      bc.columns.should == []
    end
    
    it "has an array of record_ids, that begins empty" do
      bc = BulkCopy.new(connection, source_table, dest_table)
      bc.record_ids.should == []
    end
  end
  
  describe BulkCopy, "instance record ids" do
    let(:source_table) { 'account_transactions' }
    let(:dest_table) { 'account_transactions_danced' }
    let(:columns) { ['one', 'two', 'three'] }
    
    it "can be added through #push_id" do
      id = 1
      bc = BulkCopy.new(connection, source_table, dest_table, columns)
      bc.push_id(id)
      
      bc.record_ids.should == [id]
    end

    it "appends new id with each call to #push_id" do
      id1 = 1
      id2 = 2
      
      bc = BulkCopy.new(connection, source_table, dest_table, columns)

      bc.push_id(id1)
      bc.push_id(id2)
      
      bc.record_ids.should == [id1, id2]
    end
  end
  
  describe BulkCopy, "#perform" do
    let(:source_table) { 'account_transactions' }
    let(:dest_table) { 'account_transactions_danced' }
    let(:columns) { ['one', 'two', 'three'] }
    let(:id1) { 1 }
    let(:id2) { 2 }

    it "executes a bulk copy correctly when it holds only 1 value" do
      bc = BulkCopy.new(connection, source_table, dest_table, columns)
      bc.push_id(id1)

      bc.should_receive(:execute).with("REPLACE INTO `#{dest_table}` (one,two,three) SELECT one,two,three FROM `#{source_table}` WHERE id IN (1)")
      bc.perform
    end

    it "executes a bulk copy correctly when it holds multiple sets of values" do
      bc = BulkCopy.new(connection, source_table, dest_table, columns)
      bc.push_id(id1)
      bc.push_id(id2)

      bc.should_receive(:execute).with("REPLACE INTO `#{dest_table}` (one,two,three) SELECT one,two,three FROM `#{source_table}` WHERE id IN (1,2)")
      bc.perform
    end
    
    it "executes nothing if there are no record ids" do
      bc = BulkCopy.new(connection, source_table, dest_table, columns)

      bc.should_not_receive(:execute)
      bc.perform
    end
    
    it "cannot be performed twice" do
      bc = BulkCopy.new(connection, source_table, dest_table, columns)
      bc.push_id(id1)

      bc.should_receive(:execute)
      bc.perform
      lambda { bc.perform }.should raise_error(BulkCopy::DoublePerformError)
    end
  end
  
end