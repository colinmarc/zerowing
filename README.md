ZeroWing
========

ZeroWing is a tool for copying and streaming data from MongoDB into HBase.

![all your hbase are belong to us](http://i.imgur.com/662Pur1.jpg)

Building
--------

You may need to edit `hadoop.version` and `hbase.version` in `pom.xml`, but then it should just be

    $ mvn package

Be warned that maven will also try to run the tests. Unless you have a MongoDB instance running locally, they'll automatically skip. If you do have a MongoDB instance, it'll create a `_test_zerowing` database and dump some some data into it, but it should clean up after itself just fine. Either way, expect some harmless spew.

Copying
-------

`BulkImportRunner` and `BulkImportJob` provide some nice wrappers around [Mongo-Hadoop](https://github.com/mongodb/mongo-hadoop) and the HBase output format. You can use them to easily write MapReduce jobs which map over your mongo collection and dump it into HBase.

You can use the bulk import tool straight from the command line, like so (your classpath stuff may vary):

    $ export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:`hbase classpath`
    $ hadoop jar target/zerowing-0.2.0-with-deps.jar \
      --mongo mongodb://localhost:27017/?slaveOk=true \
      --database foo   \ # optional; omit it to import every collection in every database
      --collection bar \ # optional; omit it to import every collection in the specified database

It'll look for the HBase configuration (to know where the master is) on your classpath, so you should run it this way somewhere on your HBase cluster.

You can also use it as a library:

    BulkImportRunner runner = new BulkImportRunner();

    // add a job for every collection in the database "foo"
    runner.addJobsForNamespace("mongodb://localhost:27017/?slaveOk=true", "foo", null);

    // add a job for baz.qux
    runner.addJob("mongodb://localhost:27017/baz.qux?slaveOk=true")

    // go go go!
    runner.doBulkImport();

Streaming
---------

ZeroWing can also pretend to be a MongoDB replica set member, and tail the replication oplog. This can be used to keep HBase synchronized with MongoDB in real time, as if it were another secondary to your MongoDB primary. Again, you can just use it from the command line:

    $ java -cp `hbase classpath`:target/zerowing-0.2.0-with-deps.jar com.stripe.zerowing.Tailer \
      --mongo mongodb://replica1:27017/?slaveOk=true

This will stream any inserts/updates/deletes in the oplog to the corresponding HBase tables. You can also, of course, use it as a library.

A Note on Replica Sets
----------------------

The MongoDB instance you point it at has to be a member of a replica set, or it can be an entire replica set. In that case, the url might look like:

    mongodb://replica1:8080,replica2:8080,replica3:8080/?slaveOk=true

There's more information on MongoDB replica sets [here](http://docs.mongodb.org/manual/replication/).

Configuration
-------------

Most configuration for ZeroWing can be done with Hadoop `Configuration` instances. You can use `ConfigUtil` to set any interesting options:

    import org.apache.conf.Configuration;
    import org.apache.hadoop.hbase.HBaseConfiguration;

    import com.stripe.zerowing.ConfigUtil;
    import com.stripe.zerowing.BulkImportRunner;

    Configuration conf = HBaseConfiguration.create();
    ConfigUtil.setPresplitTable(conf, false);
    ConfigUtil.setTranslatorClass(conf, MyTranslator.class);

    BulkImportRunner runner = new BulkImportRunner(conf);
    ...

Which is a nice segue into...

Translators
-----------

ZeroWing uses the `Translator` interface to convert MongoDB namespaces and blobs into the HBase equivalents. A `BasicTranslator` class comes with the package to provide some sane default behavior, but you can also extend that with your own class and provide your own behavior (to specify a class on the command line, use the `--translator` option).

Here are the methods provided by the interface, and their default implementations (in `BasicTranslator`):

  - `String mapNamespaceToHBaseTable(String database, String collection)`: this decides what to name an HBase table, given the MongoDB collection being copied or tailed. If you return `null` from this method, the collection will be skipped. By default, tables are named `zw.<database>.<collection>`.

  - `HTableDescriptor describeHBaseTable(String tableName)`: override this to create column families and such on the table, or to set options like `MAX_VERSIONS`. By default, no options are set, and only a single column family/ is created (`z`).

  - `byte[] createRowKey(BSONObject object)`: map an object in MongoDB to the right row in HBase. By default, this will take an md5sum of the `_id` (the hash is to prevent region hotspotting).

  - `Put createPut(byte[] row, BSONObject object)`: convert a BSON object to a Put, to dump into HBase. By default, this takes the raw bytes of the BSON (unconverted) and saves it to the column `z:w` of the specified row.
