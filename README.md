# timescaledb-extras

This repository includes helper functions and procedures for TimescaleDB,
particularly as a staging ground for features not ready or appropriate for the
core database, including those written in PL/pgSQL.

The current list of "extras" include:

- Procedures to help backfill data into compressed ranges

## Useful views

In the `views/` directory, there is a number of views that can be
useful. The views are typically added as separate files to allow you
to just include the views that you're interested in.

`views/chunks.sql`
: Defines views to get information about the time ranges and tablespace for chunks.

`views/missing_dimension_slices.sql`
: Defines a view `missing_dimension_slices` to check for missing dimension slices.

## Procedures

`procs/repair_dimension_slice.sql`
: Procedure `repair_dimension_slice` that will repair the `dimension_slice` table if it missing slices.

## Documentation and Help

- [Why use TimescaleDB?](https://tsdb.co/GitHubTimescaleIntro)
- [Writing data](https://tsdb.co/GitHubTimescaleWriteData)
- [Querying and data analytics](https://tsdb.co/GitHubTimescaleReadData)
- [Tutorials and sample data](https://tsdb.co/GitHubTimescaleTutorials)
- [Community Slack Channel](https://slack.timescale.com)

## Contributing

We welcome contributions to TimescaleDB Extras. The same [Contributor's
Agreement](//github.com/timescale/timescaledb/blob/master/CONTRIBUTING.md)
applies as in TimescaleDB; please sign the [Contributor License
Agreement](https://cla-assistant.io/timescale/timescaledb-extras) (CLA) if
you're a new contributor.
