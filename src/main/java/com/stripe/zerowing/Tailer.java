package com.stripe.zerowing;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang.ClassUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.bson.types.BSONTimestamp;

import com.mongodb.BasicDBObject;
import com.mongodb.Bytes;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoURI;
import com.mongodb.ServerAddress;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

public class Tailer {
  private static final byte[] STATE_TABLE_COL_FAMILY = "t".getBytes();
  private static final byte[] STATE_TABLE_COL_QUALIFIER_OPTIME = "optime".getBytes();
  private static final byte[] STATE_TABLE_COL_QUALIFIER_INC = "inc".getBytes();

  private static final Log log = LogFactory.getLog(Tailer.class);

  private final Configuration _conf;
  private final Mongo _mongo;
  private final HConnection _hbase;
  private final HashMap<String, HTable> _knownTables;
  private final HTable _stateTable;
  private final Translator _translator;

  private final boolean _skipUpdates;
  private final boolean _skipDeletes;

  private final AtomicBoolean _running = new AtomicBoolean(false);
  private int _optime = 0;
  private int _inc = 0;

  public Tailer(Configuration conf, Mongo mongo, HConnection hbase) {
    _conf = conf;
    _mongo = mongo;
    _hbase = hbase;
    _knownTables = new HashMap<String, HTable>();
    _translator = ConfigUtil.getTranslator(_conf);

    _stateTable = createStateTable();

    _skipUpdates = ConfigUtil.getSkipUpdates(_conf);
    _skipDeletes = ConfigUtil.getSkipDeletes(_conf);
  }

  public void tail() {
    if(!_running.compareAndSet(false, true)) return;

    DBCursor cursor = createCursor();
    BasicDBObject doc;

    try {
      while(_running.get() && cursor.hasNext()) {
        doc = (BasicDBObject) cursor.next();

        try {
          handleOp(doc);
        } catch (Exception e) {
          log.error("Failed to handle op: " + doc, e);
        }
      }
    } finally {
      saveOptime();
      cursor.close();
    }
  }

  public void stop() {
    log.info("Shutting down...");
    _running.set(false);
  }

  private DBCursor createCursor() {
    DBCollection oplog = _mongo.getDB("local").getCollection("oplog.rs");
    BSONTimestamp startingTimestamp = getStartingTimestamp();

    DBCursor cursor;
    if (startingTimestamp == null) {
      log.info("Tailing the oplog from the beginning...");
      cursor = oplog.find();
    } else {
      log.info("Tailing the oplog from " + startingTimestamp);
          BasicDBObject query = new BasicDBObject("ts", new BasicDBObject("$gt", startingTimestamp));
          cursor = oplog.find(query);
    }

    cursor.addOption(Bytes.QUERYOPTION_NOTIMEOUT);
    cursor.addOption(Bytes.QUERYOPTION_TAILABLE);
    cursor.addOption(Bytes.QUERYOPTION_OPLOGREPLAY);
    cursor.addOption(Bytes.QUERYOPTION_AWAITDATA);

    return cursor;
  }

  protected void handleOp(BasicDBObject doc) {
    String type = (String) doc.getString("op"),
           ns = (String) doc.getString("ns");

    updateOptime(doc);

    if (type.equals("n") || type.equals("c")) return;

    String[] parts = ns.split("\\.", 2);
    String database = parts[0], collection = parts[1];
    if (collection.startsWith("system.")) return;

    String tableName = _translator.mapNamespaceToHBaseTable(database, collection);
    HTable table;

    // skip tables that we're skipping
    if (tableName == null) return;

    try {
      table = ensureTable(tableName);
    } catch (Exception e) {
      log.error("Failed to create table " + tableName + " for op: " + doc, e);
      return;
    }

    DBObject data = (DBObject) doc.get("o");
    if (type.equals("i")) {
      handleInsert(table, data);
    } else if (!_skipUpdates && type.equals("u")) {
      DBObject selector = (DBObject) doc.get("o2");
      handleUpdate(table, data, selector, database, collection);
    } else if (!_skipDeletes && type.equals("d")) {
      handleDelete(table, data);
    }
   }

  protected void handleInsert(HTable table, DBObject doc) {
    byte[] row = _translator.createRowKey(doc);
    Put put = _translator.createPut(row, doc);

    try {
      table.put(put);
    } catch (IOException e) {
      log.error("Failed trying to insert object at " + row + " in " + table, e);
    }
  }

  protected void handleUpdate(HTable table, DBObject doc, DBObject selector,
                String database, String collection) {
    // for $set, etc, grab the whole document from mongo
    boolean resync = false;
    for (String key : doc.keySet()) {
      if (key.startsWith("$")) {
        resync = true;
        break;
      }
    }

    Object id = selector.get("_id");

    if (resync) {
      DBObject query = new BasicDBObject("_id", id);
      doc = _mongo.getDB(database).getCollection(collection).findOne(query);

      // the document may have since been removed
      if (doc == null) {
        handleDelete(table, selector);
        return;
      }
    } else {
      // the document itself is usually missing _id (but it's always present in the selector)
      doc.put("_id", id);
    }

    byte[] row = _translator.createRowKey(selector);
    Put put = _translator.createPut(row, doc);

    try {
      table.put(put);
    } catch (IOException e) {
      log.error("Failed trying to update object at " + row + " in " + table, e);
    }
  }

  protected void handleDelete(HTable table, DBObject selector) {
    byte[] row = _translator.createRowKey(selector);
    Delete del = new Delete(row);

    try {
      table.delete(del);
    } catch (IOException e) {
      log.error("Failed trying to delete object at " + row + " in " + table, e);
    }
  }

  private HTable ensureTable(String tableName) throws Exception {
    if (_knownTables.containsKey(tableName)) {
      return _knownTables.get(tableName);
    }

    HBaseAdmin admin = getHBaseAdmin();
    if (!admin.tableExists(tableName)) {
      HTableDescriptor tableDesc = _translator.describeHBaseTable(tableName);
      admin.createTable(tableDesc);
    }

    HTable table = new HTable(_conf, tableName);
    _knownTables.put(tableName, table);
    return table;
  }

  private void updateOptime(BasicDBObject doc) {
    BSONTimestamp ts = (BSONTimestamp) doc.get("ts");
    int old_optime = _optime;
    _optime = ts.getTime();
    _inc = ts.getInc();

    // only save to disk every 60 seconds
    if ((_optime - old_optime) >= 60) {
      log.info("optime: " + _optime);
      saveOptime();
    }
  }

  private void saveOptime() {
    Put put = new Put(getTailerID().getBytes());
    put.add(STATE_TABLE_COL_FAMILY, STATE_TABLE_COL_QUALIFIER_OPTIME, Integer.toString(_optime).getBytes());
    put.add(STATE_TABLE_COL_FAMILY, STATE_TABLE_COL_QUALIFIER_INC, Integer.toString(_inc).getBytes());

    try {
      _stateTable.put(put);
    } catch (IOException e) {
      log.error("Failed writing optime to state table!", e);
    }
  }

  private HBaseAdmin getHBaseAdmin() {
    try {
      return new HBaseAdmin(_hbase);
    } catch (Exception e) {
      throw new RuntimeException("Failed to (re-)connect to HBase", e);
    }
  }

  private String getTailerID() {
    List<ServerAddress> addresses = _mongo.getAllAddress();
    return StringUtils.join(addresses, ",");
  }

  private HTable createStateTable() {
    String stateTableName = ConfigUtil.getTailerStateTable(_conf);
    HBaseAdmin admin = getHBaseAdmin();

    try {
      if (!admin.tableExists(stateTableName)) {
        HTableDescriptor tableDesc = new HTableDescriptor(stateTableName);
        HColumnDescriptor familyDesc = new HColumnDescriptor(STATE_TABLE_COL_FAMILY);
        familyDesc.setMaxVersions(1);
        tableDesc.addFamily(familyDesc);

        admin.createTable(tableDesc);
      }

      return new HTable(_conf, stateTableName);
    } catch (Exception e) {
      throw new RuntimeException("Failed to create state table", e);
    }
  }

  private BSONTimestamp getStartingTimestamp() {
    Get get = new Get(getTailerID().getBytes());

    Result res;
    try {
      res = _stateTable.get(get);
    } catch (IOException e) {
      log.error("Failed to get a starting timestamp for tailer ID: " + getTailerID());
      return null;
    }

    if (res.isEmpty()) {
      if(ConfigUtil.getSkipBacklog(_conf)) return new BSONTimestamp((int) (System.currentTimeMillis() / 1000L), 0);
      return null;
    } else {
      byte[] raw_optime = res.getValue(STATE_TABLE_COL_FAMILY, STATE_TABLE_COL_QUALIFIER_OPTIME);
      byte[] raw_inc = res.getValue(STATE_TABLE_COL_FAMILY, STATE_TABLE_COL_QUALIFIER_INC);

      int optime = Integer.parseInt(new String(raw_optime));
      int inc = Integer.parseInt(new String(raw_inc));
      return new BSONTimestamp(optime, inc);
    }
  }

  private static class CommandLineOptions {
    @Parameter(names = {"-m", "--mongo"}, description = "The base MongoDB URI to connect to", required = true)
    private String mongoURI;

    @Parameter(names = {"-t", "--translator"}, description = "Specify a ZWTranslator class to use. Defaults to com.stripe.zerowing.ZWBasicTranslator")
    private String translatorClass;

    @Parameter(names = "--skip-updates", description = "Skip update operations - when a record is updated in MongoDB, don't update it in HBase")
    private boolean skipUpdates;

    @Parameter(names = "--skip-deletes", description = "Skip delete operations - when a record is deleted in MongoDB, don't delete it in HBase")
    private boolean skipDeletes;

    @Parameter(names = "--help", help = true, description = "Show this message")
    private boolean help;
  }

  public static void main(String[] args) throws Exception {
    CommandLineOptions opts = new CommandLineOptions();
    JCommander parser = new JCommander(opts, args);

    if (opts.help) {
      parser.usage();
      return;
    }

    Configuration conf = HBaseConfiguration.create();

    if (opts.translatorClass != null) {
      log.info("Setting the translator class to " + opts.translatorClass);

      try {
        @SuppressWarnings("unchecked")
        Class<? extends Translator> translatorClass = ClassUtils.getClass(opts.translatorClass);
        ConfigUtil.setTranslatorClass(conf, translatorClass);
      } catch (ClassNotFoundException e) {
        log.error("Couldn't find translator class: " + opts.translatorClass, e);
        return;
      }
    }

    if (opts.skipUpdates) {
      ConfigUtil.setSkipUpdates(conf, true);
    }

    if (opts.skipDeletes) {
      ConfigUtil.setSkipDeletes(conf, true);
    }

    @SuppressWarnings("deprecation")
    MongoURI uri = new MongoURI(opts.mongoURI);
    Mongo mongo;
    HConnection hbase;

    try {
      mongo = uri.connect();
    } catch (UnknownHostException e) {
      log.error("Failed to connect to MongoDB using uri: " + uri, e);
      return;
    }

    try {
      hbase = HConnectionManager.createConnection(conf);
    } catch (Exception e) {
      log.error("Failed to connect to HBase", e);
      return;
    }

    Tailer tailer = new Tailer(conf, mongo, hbase);
    Thread currentThread = Thread.currentThread();
    Runtime.getRuntime().addShutdownHook(new TailerCleanupThread(tailer, currentThread));

    try {
      tailer.tail();
    } finally {
      hbase.close();
    }

    return;
  }

  private static class TailerCleanupThread extends Thread {
    private final Tailer _tailer;
    private final Thread _tailerThread;

    public TailerCleanupThread(Tailer tailer, Thread tailerThread) {
      _tailer = tailer;
      _tailerThread = tailerThread;
    }

    public void run() {
      try {
        _tailer.stop();
        _tailerThread.join();
      } catch (InterruptedException e) {
        log.info("Going down hard!");
      }
    }
  }
}
