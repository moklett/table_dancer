class TestMigration < ActiveRecord::Migration
  def self.up
    create_table :table_dances, :force => true do |t|
      t.string  :source_table
      t.string  :dest_table
      t.string  :phase, :default => 'init'
      t.integer :last_copy_id
      t.timestamps
    end
    
    add_index :table_dances, :source_table
    add_index :table_dances, :phase
    add_index :table_dances, [:source_table, :phase]
    
    create_table :table_dance_replays, :force => true do |t|
      t.integer   :table_dance_id      # We support multiple table dances in the same replays table
      t.integer   :instruction         # 1 = Insert, 2 = Update, 3 = Delete
      t.datetime  :event_time          # "Original time" of event (created_at for insert, current timestamp for Update/Delete
      t.integer   :source_id           # ID of row in source table
      t.boolean   :performed, :default => false
    end
    
    add_index :table_dance_replays, :table_dance_id
    add_index :table_dance_replays, :event_time
    add_index :table_dance_replays, [:table_dance_id, :event_time]
    add_index :table_dance_replays, :performed
    add_index :table_dance_replays, [:table_dance_id, :event_time, :performed], :name => 'by_dance_time_and_performed'
    add_index :table_dance_replays, :source_id
    add_index :table_dance_replays, :instruction
    add_index :table_dance_replays, [:table_dance_id, :instruction], :name => 'by_dance_instr'
    add_index :table_dance_replays, [:table_dance_id, :event_time, :instruction], :name => 'by_dance_time_instr'
    add_index :table_dance_replays, [:table_dance_id, :event_time, :instruction, :performed], :name => 'by_dance_time_instr_performed'
  end
  
  def self.down
    drop_table :table_dances
    drop_table :table_dance_replays
  end
end