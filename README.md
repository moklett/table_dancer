Hold me closer, table dancer
============================

    rails g table_dancer:migration
    # Creates a migration that creates the table_dances and table_dance_replays tables
    
    TableDance.start!('account_transaction')
    #
    
Phases
------

1. Init
  * Acquire write lock on source table
  * Record initial state (last_copy_id)
2. Trigger
  * Install Insert/Update/Delete triggers
    * The triggers each create replays for the change that they observed
  * Release write lock
3. Copy
  * Walk source table rows up to last_copy_id
  * Create an Insert replay for each row
4. Replay
  * Walk replays table in order of event_time, considering unperformed replays
    * Insert replays: create new destination record with matching primary key of source record
    * Update replays: update existing destination record
    * Delete replays: delete existing destination record
  * Mark each replayed row as performed
  * Repeat replay until less than N unperformed replays exist
5. Cutover
  * Acquire write lock on source table
  * Perform final N replays
  * Rename source table to *_decommisioned
  * Rename ghost table to original source table name
  * Release write lock


    
Table Dancer Schema
-------------------

    table "table_dances"
      integer   id
      string    source_table
      string    dest_table
      integer   phase
      integer   last_copy_id          # ID of last record that needs to be considered during Copy phase
      
    table "table_dance_replays"
      integer   id
      integer   table_dance_id        # We support multiple table dances in the same replays table
      integer   instruction           # 1 = Insert, 2 = Update, 3 = Delete
      datetime  event_time            # "Original time" of event (created_at for insert, current timestamp for Update/Delete
      integer   source_id             # ID of row in source table
      boolean   performed
      
      

References
----------

[MySQL at Facebook's OSC (Online Schema Change)](http://www.facebook.com/notes/mysql-at-facebook/online-schema-change-for-mysql/430801045932)
[openark kit's oak-online-alter-table](http://openarkkit.googlecode.com/svn/trunk/openarkkit/doc/html/oak-online-alter-table.html)
[TableMigrator](https://github.com/freels/table_migrator)

### Correct way to lock InnoDB tables

From <http://dev.mysql.com/doc/refman/5.0/en/lock-tables-and-transactions.html>

    SET autocommit=0;
    LOCK TABLES t1 WRITE, t2 READ, ...;
    ... do something with tables t1 and t2 here ...
    COMMIT;
    UNLOCK TABLES;
    
See also `in_table_lock`: <https://github.com/freels/table_migrator/blob/master/lib/table_migrator/copy_engine.rb#L265>