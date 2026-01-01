# timescaledb-extras

This repository includes helper functions and procedures for TimescaleDB,
particularly as a staging ground for features not ready or appropriate for the
core database, including those written in PL/pgSQL.

The current list of "extras" include:

## Diagnostic checks

`diagnostic.sql`
: Checks for common misconfigurations and issues that can affect
performance and reliability of TimescaleDB installations.

## Useful views

In the `views/` directory, there is a number of views that can be
useful. The views are typically added as separate files to allow you
to just include the views that you're interested in.

`chunks.sql`
: Defines views to get information about the time ranges and tablespace for chunks.

## Useful utilities

`compression.sql`
: Define utilities for working with compressed tables.

`migrate.sql` ([documentation](docs/migrate.md))
: Incrementally copy existing time-series data from large tables into a new hypertable.

## Documentation and Help

- [Why use TimescaleDB](https://www.tigerdata.com/case-studies)
- [Writing data](https://www.tigerdata.com/docs/use-timescale/latest/write-data)
- [Querying and data analytics](https://www.tigerdata.com/docs/use-timescale/latest/query-data)
- [Tutorials and sample data](https://www.tigerdata.com/docs/tutorials/latest)
- [Community Slack Channel](https://slack.timescale.com)

## Contributing

We welcome contributions to TimescaleDB Extras. The same [Contributor's
Agreement](//github.com/timescale/timescaledb/blob/master/CONTRIBUTING.md)
applies as in TimescaleDB; please sign the [Contributor License
Agreement](https://cla-assistant.io/timescale/timescaledb-extras) (CLA) if
you're a new contributor.
