# `migrate.sql` README
Often users of TimescaleDB have large tables of pre-existing data that they wish to
migrate into a hypertable. Without involving external tools to manage the copy of 
data, there are generally two existing options for converting this data into a new hypertable.

**Option 1: Use the `migrate_data` options of `create_hypertable()`**

The create_hypertable() function does provide an option that attempts to automate
the migration of existing data for you. Unfortunately, the process is inefficient 
for large datasets and provides no mechanism to the progress of the migration. And
finally, the process is done in a single transaction which will rollback all migrated
data if the process fails for any reason before completing.

**Option 2: `SELECT` existing data `INTO` a new hypertable**

A second alternative is to create a new, empty hypertable, and then perform a 
`INSERT INTO...SELECT` type query. This is effectively a manual approach to using
the `migrate_data` option and generally suffers from the same limitations.

## A better alternative: `migrate.sql` functions and procedure
Instead, you can use the functions provided in the `migrate.sql` file to scan an existing
table that contains time-series data, create a "control" table that divides the 
total span of time into small ranges of a few hours or days, and then use multiple 
sessions (if you want) to copy data through a series of transactions.

This provides a few advantages:
 * Each small batch of data is committed in it's own transaction. This generally 
   leads to a faster overall process
 * Because each small range of data is copied in a separate transaction, data that
   has already been copied will not be rolled back if something fails part way
   through the overall process
 * The process can be restarted to pick up where it left off without data loss
   or duplication
 * The copy process can use multiple sessions to parallelize the copy process.

### Limitations
In this first iteration, the process does not allow you to provide a custom
`SELECT` statement for doing the data migration. Therefore, the source and 
target tables must have columns in the same order. This may be improved in the
future.

## Using the `migrate.sql` process
The basic process for migrating data wit the functions in this script include:

1. Prepare the source table with proper indexes if necessary
1. Create a new, empty hypertable with the same schema as the source table.
1. Run the `migrate.sql` script to create the stored procedure and helper
functions in the database to facilitate data migration
1. `CALL` the `migrate_to_hypertable()` function with the parameters outlined
below.

Let's look at each step.

### Step 1: Prepare the source table
Assume that you have a source table with existing time-series data and the 
following schema:

```sql
CREATE TABLE sensor_data (
  ts TIMESTAMPTZ NOT NULL,
  sensor_id INTEGER,
  temperature DOUBLE PRECISION,
  cpu DOUBLE PRECISION
);
```

Before migrating data, we recommend adding an index with `time` as the leading
column if the table does not already have one. This should improve the overall
performance of the insert process, however, on a large, existing table, this
may take a significant amount of time. Prepare accordingly.

### Step 2: Create a new hypertable with the same schema as the source table
First, create a copy of the source table with a `CREATE TABLE... LIKE` command
to ensure proper order and data types.

```sql
CREATE TABLE sensor_data_new (LIKE sensor_data INCLUDING DEFAULTS INCLUDING CONSTRAINTS EXCLUDING INDEXES);
```

Create a hypertable from this new table. Depending on the amount and density 
(cardinality) of your data, you may want to consider using a different `chunk_time_interval`.
See the hypertable best practices documentation for more information.

```sql
SELECT create_hypertable('sensor_data_new', 'ts');
```

### Step 3: Run the `migrate.sql` script on the database
Using `psql` or another tool, run the entire `migrate.sql` script on the database
to create the main stored procedure and all helper functions.

### Step 4: Begin the migration process
The `migrate_to_hypertable()` stored procedure does two things:

1. It creates a special "log" table that identifies the total span of time in the
source table based on the timestamp column. Based on the span of time, the log
table has one entry for each batch of rows that will be copied over. If the log table
already exists, it will not be dropped and recreated. This allows you to re-run the
process again if it fails partway through.
1. Once the table is created and populated, it begins to run `INSERT INTO...SELECT`
batches for each time span in the logging table. As an `INSERT` statement finishes
it is marked as complete.

The process runs to completion unless it is stopped in some way. If it is, simply
run the same `CALL` statement again and the process will pick up where it left
off.

> By default, this will create "batches" of rows that span 1/10th the interval of
> the `chunk_time_interval` of the hypertable. Therefore, if your hypertable uses
> the default 7-day interval, then the script will select batches of rows that
> cover ~17 hours of time.

The migration process is run within a session and supports basic 
"parallelization" by running multiple sessions at the same time with different
"worker" identifiers.

### **Migrate data with a single process**
You can run the migration as a single process. This still has advantages over
running a `INSERT INTO...SELECT` statement because it will commit rows of data
while the process is running to ensure processed data is saved to disk in case
of failure.

```sql
CALL migrate_to_hypertable('sensor_data','sensor_data_new');
```

Depending on how dense your table is (how many rows per span of time), you may want
to adjust the time range for each batch of rows. In this example, we still only
run one process, but the time span of batches is only 4 hours.

```sql
CALL migrate_to_hypertable('sensor_data','sensor_data_new', '4 hours'::interval);
```

### **Migrate data with multiple sessions**
If your server has sufficient resources, you can often decrease the total time
to migrate a large amount of data by running multiple processes at the same time
from different sessions. In this scenario, you must also identify an additional
column to further break down the batches of rows. This will most likely be
the partitioning column of the hypertable (`sensor_id` in our example table)

To run a "multi-threaded" migration, determine how many sessions you will be 
running at once. To run four separate sessions, call the `migrate_to_hypertable()`
function from each session with a **different** worker number. This is essential
for the sudo-parallelization to work.

**Session 1**
```sql
CALL migrate_to_hypertable('sensor_data','sensor_data_new', '4 hours'::interval,'sensor_id',4,1);
```
**Session 2**
```sql
CALL migrate_to_hypertable('sensor_data','sensor_data_new', '4 hours'::interval,'sensor_id',4,2);
```
**Session 3**
```sql
CALL migrate_to_hypertable('sensor_data','sensor_data_new', '4 hours'::interval,'sensor_id',4,3);
```
**Session 4**
```sql
CALL migrate_to_hypertable('sensor_data','sensor_data_new', '4 hours'::interval,'sensor_id',4,4);
```

Notice that each session calls the same SQL statement with just the last "worker_id" changed.

The first process to connect to the database will create the logging table
for all other processes to use.

## Checking the migration progress
The logging table that is generated for each migration is based
on the OID of the destination hypertable. You can query the `migrated`
column to determine the overall progress of the migration.

```sql
SELECT tablename FROM pg_catalog.pg_tables 
WHERE tablename LIKE '_ts_migrate_log_%';

tablename              |
-----------------------+
_ts_migrate_log_6196337|
```

Once you have the name of the logging table, you can determine
how many batches have been migrated so far.

```sql
SELECT count(*), migrated from _ts_migrate_log_6196337
GROUP BY migrated;

count|migrated|
-----+--------+
  241|true    |
```