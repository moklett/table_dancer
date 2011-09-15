module TableDancer
  module Instructions
    INSERT = {
      'id' => 1,
      'name' => 'insert',
      'table_ref' => 'NEW'
    }
    UPDATE = {
      'id' => 2,
      'name' => 'update',
      'table_ref' => 'NEW'
    }
    DELETE = {
      'id' => 3,
      'name' => 'delete',
      'table_ref' => 'OLD'
    }
  end
end