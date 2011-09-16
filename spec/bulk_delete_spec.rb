require 'spec_helper'

module TableDancer
  describe BulkDelete, "new instance" do
    let(:table_name) { 'account_transactions' }
    
    it "has a private database connection given by the 1st argument" do
      bd = BulkDelete.new(connection, table_name)
      bd.instance_variable_get(:@connection).should == connection
    end
    
    it "has a table_name that corresponds with the 2nd argument" do
      bd = BulkDelete.new(connection, table_name)
      bd.table_name.should == table_name
    end

    it "has an array of record_ids, that begins empty" do
      bd = BulkDelete.new(connection, table_name)
      bd.record_ids.should == []
    end
  end
  
  describe BulkDelete, "instance record ids" do
    let(:table_name) { 'account_transactions' }
    
    it "can be added through #push_id" do
      id = 1
      bd = BulkDelete.new(connection, table_name)
      bd.push_id(id)
      
      bd.record_ids.should == [id]
    end

    it "appends new id with each call to #push_id" do
      id1 = 1
      id2 = 2
      
      bd = BulkDelete.new(connection, table_name)

      bd.push_id(id1)
      bd.push_id(id2)
      
      bd.record_ids.should == [id1, id2]
    end
  end
  
  describe BulkDelete, "#perform" do
    let(:table_name) { 'account_transactions' }
    let(:id1) { 1 }
    let(:id2) { 2 }

    it "executes a bulk delete correctly when it holds only 1 value" do
      bd = BulkDelete.new(connection, table_name)
      bd.push_id(id1)

      bd.should_receive(:execute).with("DELETE FROM `#{table_name}` WHERE id IN (1)")
      bd.perform
    end

    it "executes a bulk delete correctly when it holds multiple sets of values" do
      bd = BulkDelete.new(connection, table_name)
      bd.push_id(id1)
      bd.push_id(id2)

      bd.should_receive(:execute).with("DELETE FROM `#{table_name}` WHERE id IN (1,2)")
      bd.perform
    end
    
    it "does nothing when there are no record ids" do
      bd = BulkDelete.new(connection, table_name)

      bd.should_not_receive(:execute)
      bd.perform
    end
    
    it "cannot be performed twice" do
      bd = BulkDelete.new(connection, table_name)
      bd.push_id(id1)

      bd.should_receive(:execute).once
      bd.perform
      lambda { bd.perform }.should raise_error(BulkDelete::DoublePerformError)
    end
    
  end
  
end