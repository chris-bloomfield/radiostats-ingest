## radiostats-ingest

Script to convert & export RAJAR data to a MongoDB database.

To run:
`yarn ingest`

This will:
 - Create a `radio-stats` database containing `nameChanges`, `results`, and `stations` collections (if these already exist they will be deleted)
 - Import data form any valid CSV files in the `/input` directory & check it for consistency against any name change data in `/data/nameChanges`
 - Convert CSV data to the DB schema & insert it in the database.
