A python project for the modio housekeeper.

# What it does

It runs Zabbix housekeeping tasks in a Postgres database.

## housekeeper

1. Creates new (Future) partitions for the history tables
2. Cleans out large indexes (btree) from older partitions
3. Creates smaller (brin) indexes on older partitions
4. Cleans out older data (removed, not used) from all partitions

## Retention

Takes a configuration variable for how long (in days) to keep data, via the 
environment.  It will remove partitions older than that.


## Deletion

Maintenance executes in practice at two times (but is scheduled for every day):
- the 14th of a month
- When archiving data

expiration is handled at both these times, with different retention times:
Items with a retention time (`item.history  < retention`) less than the current
retention are selected for expiration, and data points that are older than
`retention` days old at this time are deleted.

This makes it that we right now only care for three kinds of values of item.history:
- Below 14 days  (Cleaned the 14th every month)
- Below MODIO_ARCHIVE days (cleaned when data is migrated)
- Above MODIO_ARCHIVE days (never expired)

Archiving is defined as migrating the months that are _older_ than the
MODIO_ARCHIVE retention period. This makes every value in the migrated data set
older than MODIO_ARCHIVE days.

At this point, since we're already touching all parts of the table, we'll
run an expiry of old data (items with a history defined as smaller than
retention period), and will delete all posts for those items that are older
than expected, but _only_ on the tables we will migrate.

The 14th of the month, we run maintenance on last months data. To do that, we
reindex the table, expire data, clean out duplicates, and cluster the table in
(itemid,clock) order for efficient queries.

If you do not run the archiver, then expiration only happens for data sets < 14
days old.

## Archiver

The archiver tool moves data into an `archive` database.  


This requires a LOT of setup between the two, parts of which is documented in
the one time setup job(s).

To get the one-time setup output for the archive db, run `migration setup_archive`.
This creates a default user with a default password. Replace this password consistently.

Then, on the "primary" database ( the one hosting your live data), run 
`migration setup_migration`  to set up the same (matching) users & databases 


After this, you can run the `migration oneshot_archive` to generate tables for
the year 2014 and until today's date. 2014 was picked because we don't have 
older data than that.


Then, you can do a one-time archive ( move all existing data ) by running
`migration slow_migrate`. This is really REALLY slow, you're usually better off
importing a backup ( `psql copy to | psql copy from` ).


The actual offloading assumes you're already using a partitioned database with
our namingscheme for partitions. Make sure it's run with the correct user, or
your permissions will be off.

## DB setup notes

archive DB needs pg_hba setup with users, database an others from:

* main db
* machine where maintenance job goes

Since the maintenance job (archiver) connects to both main and alternate db to
transfer data


## Setup use performs:    

1. create db user on archive db server
2. create db on archive db server
3. create foreign data wrapper on primary server
4. create user mappings on primary server

# Oneshot archive

5. Outputs SQL to create archive tables on archive db


## cron use: 

Set up tool to run weekly or similar, and set up a time limit for your
retention. The cron job will iterate over all partitions older than the archive
retention, and migrate them to the archive DB. This will be done first with a
COPY job, and then a second pass select/insert, and clean out of the table.


Open questions:

This doesn't quite work together with the retention tool. Should the two be
merged together? (either you have archival db, or you have retention.  Don't do
both, as the retention tool doesn't know about archive tables )
