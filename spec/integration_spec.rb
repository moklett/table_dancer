require 'spec_helper'
require 'md5'

module TableDancer
  describe "TableDancer integration test" do
    include DummyClasses

    let(:table_name) { 'account_transactions' }
    let(:schema) { {:string => 'type', :integer => 'invoice_id', :boolean => 'success', :decimal => 'amount', :text => 'message'} }
    let(:starting_dataset_size) { 20 }
    let(:klass) { dummy_class_for(table_name) }
    
    before do
      TableDancer.rest_interval = 3
      setup_tables
      generate_initial_dataset
    end

    after do
      remove_tables
    end
    
    it "faithfully reproduces a table that is accepting live inserts" do
      # Remember...
      last_copy_id = klass.first(:order => "id DESC").id
      original_table_checksum = table_checksum(table_name)

      # Start table dance...
      thread1 = Thread.new do
        TableDancer::TableDance.run!(table_name)
      end

      # While continuing to insert new rows
      thread2 = Thread.new do
        continually_perform('insert')
      end

      thread1.join
      thread2.join
      
      # it "creates a table that exactly matches the decommissioned table" 
      new_source_table_checksum = table_checksum(table_name)
      archived_table_checksum = table_checksum("#{table_name}_decommissioned")
      new_source_table_checksum.should == archived_table_checksum

      # it "does not alter any rows that were orignally in the table"
      archived_partial_table_checksum = table_checksum("#{table_name}_decommissioned", last_copy_id)
      archived_partial_table_checksum.should == original_table_checksum

      # it "generates a final table that is larger than the original table"
      dummy_class_for(table_name).count.should be > starting_dataset_size
    end
    
    it "faithfully reproduces a table that is accepting live inserts, updates, and deletes" do
      # Start table dance...
      thread1 = Thread.new do
        TableDancer::TableDance.run!(table_name)
      end

      # While continuing to insert, update, and delete rows
      thread2 = Thread.new do
        continually_perform('insert', 'update', 'delete')
      end

      thread1.join
      thread2.join
      
      # it "creates a table that exactly matches the decommissioned table" 
      new_source_table_checksum = table_checksum(table_name)
      archived_table_checksum = table_checksum("#{table_name}_decommissioned")
      new_source_table_checksum.should == archived_table_checksum
    end
    
    def setup_tables
      remove_tables

      [table_name, "#{table_name}_danced"].each do |name|
        init_table(name, schema)
      end
    end
    
    def generate_initial_dataset
      starting_dataset_size.times do
        random_new_row
      end
      klass.count.should == starting_dataset_size
    end
    
    def random_new_row
      klass.create do |k|
        k['type']     = ['Charge', 'Payment', 'Adjustment'][rand(3)]
        k.invoice_id  = rand(10000)
        k.success     = [true, false][rand(2)]
        k.amount      = rand(1000).to_f + rand(0)
        k.message     = "x"*rand(100)
      end
    end
    
    def randomly_alter_row
      row = random_row
      row.update_attribute(:message, "#{row.message}#{'x'*rand(10)}")
    end

    def randomly_delete_row
      random_row.destroy
    end
    
    def random_row
      random_id = rand(klass.count)
      klass.first(:conditions => "id >= #{random_id}") || klass.first
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
    
    def remove_tables
      [table_name, "#{table_name}_danced", "#{table_name}_decommissioned"].each do |name|
        connection.drop_table(name) if connection.table_exists?(name)
      end
    end
    
    def continually_perform(*actions)
      # Continue performing actions until we're part way in to the replay phase
      # (Then quiesce so we can have success comparing tables)
      while true
        dance = TableDancer::TableDance.first(:conditions => {:source_table => table_name})
        if dance
          phase = dance.phase
        else
          phase = 'init'
        end
        break if %w(cutover complete).include?(phase)
        unless phase == 'init'
          action = actions[rand(actions.size)]
          case action
          when 'insert'
            random_new_row
          when 'update'
            randomly_alter_row
          when 'delete'
            randomly_delete_row
          end
        end
        sleep 1
      end
    end
  end
end
