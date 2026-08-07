# Issue downgrading from 2.22.0 due to unrefreshed CAggs

In 2.22.0 we introduced an additional transaction during the CAgg refresh which processes ranges from a new table `_timescaledb_catalog.continuous_aggs_materialization_ranges`.
This staging table was introduced in order to help fix issues relating to concurrent CAgg refreshes: [#8372](https://github.com/timescale/timescaledb/issues/8372), [#8490](https://github.com/timescale/timescaledb/issues/8490)

Since this table would be dropped on a downgrade to 2.21.3, we block the downgrade if there are any ranges present in the table, in order to ensure no data loss in dependent CAggs. It is generally expected that this staging table will only contain ranges which are being refreshed.
If there *is* any data leftover in the table, that *isn't* being processed, it is because of an interrupted CAgg refresh, so we ask the user to perform a force refresh of these ranges before proceeding with the downgrade. In most cases, this should suffice.

However, if a force refresh of all CAggs is not a possible option, we are blocked from downgrading completely. This script provides a workaround.

**We recommend trying to force refresh all CAggs before performing the downgrade normally. This script is a workaround which serves only as a backup option.**

## How it works

The workaround is straightforward: we save a copy of the ranges in `_timescaledb_catalog.continuous_aggs_materialization_ranges` in a new table `_timescaledb_internal.saved_ranges`. We then perform the downgrade, which should go through. After this, we force-refresh CAggs using the saved ranges then drop the `saved_ranges` table.

## Usage

Start a `psql` session and do the following:

1. Copy the ranges into a new table.

```SQL
\i 01-save-materialization-ranges.sql
```


2. Open a new psql session and downgrade to timescaledb version 2.21.3:

```SQL
ALTER EXTENSION timescaledb UPDATE TO '2.21.3';
```

3. Force refresh CAggs using the saved ranges and drop the copy:

```SQL
\i 02-refresh-saved-ranges.sql
```
