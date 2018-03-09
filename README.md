A python project for the modio housekeeper


# Retention:

- Create foreign table
CREATE EXTENSION postgres_fdw;
CREATE SERVER ppam_archive
    FOREIGN DATA WRAPPER postgres_fdw OPTIONS 
            (host 'db2.foo.bar', port '5532', dbname 'ppam.modio.se');

CREATE USER MAPPING FOR PUBlIC SERVER ppam_archive OPTIONS(password 'foobar');

# TODO:  Retention / housekeeper needs to run against this second server as well
#  In order to create the tables we need

CREATE FOREIGN TABLE history_foo_bar_baz (like history) SERVER ppam_archive OPTIONS( table_name 'history_foo_bar_baz');


