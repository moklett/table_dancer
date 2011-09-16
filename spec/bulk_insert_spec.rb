require 'spec_helper'

module TableDancer
  describe BulkInsert, "new instance" do
    let(:table_name) { 'account_transactions' }
    let(:columns) { ['one', 'two', 'three'] }
    
    it "has a private database connection given by the 1st argument" do
      bi = BulkInsert.new(connection, table_name)
      bi.instance_variable_get(:@connection).should == connection
    end
    
    it "has a table_name that corresponds with the 2nd argument" do
      bi = BulkInsert.new(connection, table_name)
      bi.table_name.should == table_name
    end
    
    it "has a list of columns that correspond with the 3rd argument" do
      bi = BulkInsert.new(connection, table_name, columns)
      bi.columns.should == columns
    end
    
    it "allows you to set the columns through an accessor" do
      bi = BulkInsert.new(connection, table_name)
      bi.columns = columns
      bi.columns.should == columns
    end
    
    it "holds an empty array of columns if they are not set" do
      bi = BulkInsert.new(connection, table_name)
      bi.columns.should == []
    end
    
    it "has an array of value sets, that begins empty" do
      bi = BulkInsert.new(connection, table_name)
      bi.value_sets.should == []
    end
  end
  
  describe BulkInsert, "instance value sets" do
    let(:table_name) { 'account_transactions' }
    let(:columns) { ['one', 'two', 'three'] }
    
    it "can be added through #push_values (single set)" do
      value_set = ['a', 'b', 'c']
      bi = BulkInsert.new(connection, table_name, columns)
      bi.push_values(value_set)
      
      bi.value_sets.should == [value_set]
    end

    it "appends new sets with each call to #push_values" do
      value_set1 = ['a', 'b', 'c']
      value_set2 = ['d', 'e', 'f']
      
      bi = BulkInsert.new(connection, table_name, columns)

      bi.push_values(value_set1)
      bi.push_values(value_set2)
      
      bi.value_sets.should == [value_set1, value_set2]
    end
  end
  
  describe BulkInsert, "#perform" do
    let(:table_name) { 'account_transactions' }
    let(:columns) { ['one', 'two', 'three'] }
    let(:value_set1) { ['a', 'b', 'c'] }
    let(:value_set2) { ['d', 'e', 'f'] }

    it "executes a bulk insert correctly when it holds only 1 set of values" do
      bi = BulkInsert.new(connection, table_name, columns)
      bi.push_values(value_set1)

      bi.should_receive(:execute).with("INSERT INTO `#{table_name}` (one,two,three) VALUES ('a','b','c')")
      bi.perform
    end

    it "executes a bulk insert correctly when it holds multiple sets of values" do
      bi = BulkInsert.new(connection, table_name, columns)
      bi.push_values(value_set1)
      bi.push_values(value_set2)

      bi.should_receive(:execute).with("INSERT INTO `#{table_name}` (one,two,three) VALUES ('a','b','c'),('d','e','f')")
      bi.perform
    end
    
    it "inserts nothing if no value sets exist" do
      bi = BulkInsert.new(connection, table_name, columns)

      bi.should_not_receive(:execute)
      bi.perform
    end
    
    it "cannot be performed twice" do
      bi = BulkInsert.new(connection, table_name, columns)
      bi.push_values(value_set1)

      bi.should_receive(:execute)
      bi.perform
      
      lambda { bi.perform }.should raise_error(BulkInsert::DoublePerformError)
    end
  end
  
end