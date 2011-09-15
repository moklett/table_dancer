require 'active_record'
require 'table_dancer/dummy_classes'
require 'table_dancer/instructions'
require 'table_dancer/table_dance'
require 'table_dancer/table_dance_replay'

module TableDancer
  def self.setup(table_name)
    TableDancer::TableDance.setup(table_name)
  end
end