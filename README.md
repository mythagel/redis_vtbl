redis_vtbl
==========

Overview
--------
An implementation of an [sqlite3](http://www.sqlite.org/) virtual table module that stores data in [redis](http://redis.io/).

Different sqlite database instances on (potentially) different systems with the same virtual table definition transparently share their data.

`DROP TABLE` on any of the instances individually does not remove any data.

Changes to the `CREATE TABLE` definition are minimal, consisting mainly of syntax changes to specify the virtual table module name, and configuration to connect to redis. Column specifications are unchanged.

Redis connection specification can either be a single redis instance, or (untested) a list of sentinel addresses and a service name from which to determine the active redis master.


Design Notes
------------
Intent: To provide a mechanism to existing sql based products to be modified to transparently share data.

Note that while indexes remain unimplemented performance is subpar on any queries excepting those on the rowid.

Considerations for enormous amounts of data have not been made (hence redis vs. e.g. couchdb). Goal is to provide very fast access to a bounded amount of shared runtime state via an sql api.

 * * *

There are multiple reasons you should _not_ use this library.

1. Starting with a new product where redis / KV stores are a good solution. Use redis / KV store directly.
2. Existing product using sql that uses a (central/shared) database server. This library offers no benefits to continuing to use the db server directly.
3. Existing small product that uses sql that would be a better fit for a KV store. Adapt to use KV store directly.

 * * *

So, when _should_ you use this library?

*I have no idea.*

Unless you were looking for something like this specifically then you probably shouldn't.

Want SQL use [postgresql](http://www.postgresql.org/).
Want KV (and then some) then use [redis](http://redis.io/).


Developed and tested on linux. May work elsewhere; patches welcome.

Example
-------

Original table definition:

    CREATE TABLE test0 (
       blah     VARCHAR(255),
       blah2    INTEGER,
       blah3    VARCHAR(6)
    );

Redis backed table definition:

    CREATE VIRTUAL TABLE test0 USING redis (localhost:6379, prefix,
       blah     VARCHAR(255),
       blah2    INTEGER,
       blah3    VARCHAR(6)
    );

Will be stored in the following redis keys:

    prefix.db.table:{rowid}      = hash of the row data.
    prefix.db.table.rowid        = sequence from which rowids are generated.
    prefix.db.table.index.rowid  = master index (zset) of rows in the table
    
Note: Indexes are not yet implemented.

    prefix.db.table.indices      = master index (set) of indices on the table
    prefix.db.table.index:{x}    = additional set(rowid) index(s) for column x

