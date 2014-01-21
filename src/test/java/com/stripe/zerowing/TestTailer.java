package com.stripe.zerowing;

import java.util.List;
import java.util.UUID;

import com.mongodb.*;
import org.bson.*;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;

import org.junit.*;
import static org.junit.Assert.*;
import static org.junit.Assume.*;

public class TestTailer {

  static HBaseTestingUtility util = new HBaseTestingUtility();
  static MongoClient mongo;

  String name;
  String tableName;
  TailerThread tailerThread;

  @BeforeClass
  public static void setUpClass() throws Exception {
    try {
      // try the normal way first, to avoid chunder
      MongoClient testMongoClient = new MongoClient("localhost", 27017);
      testMongoClient.getDatabaseNames();

      // this is necessary to connnect as a replica set
      mongo = new MongoClient(new MongoClientURI("mongodb://localhost:27017,localhost:27017/?slaveOk=true"));
      mongo.getDatabaseNames();
      assumeNotNull(mongo.getReplicaSetStatus());
    } catch (Exception e) {
      System.out.println("error connecting to mongo: " + e);
      assumeTrue(false);
    }

    util.startMiniCluster();
  }

  @Before
  public void setUp() throws Exception {
    try {
      mongo.getDB("_test_zerowing").dropDatabase();
    } catch(Exception e) {
      System.out.println("error setting up mongo: " + e);
      assumeTrue(false);
    }

    name = UUID.randomUUID().toString();
    tableName = "zw._test_zerowing." + name;
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    util.shutdownMiniCluster();
  }

  @Test
  public void testBasicTailer() throws Exception {
    Configuration conf = new Configuration(util.getConfiguration());
    ConfigUtil.setSkipBacklog(conf, true);

    HBaseAdmin admin = util.getHBaseAdmin();
    assertFalse(admin.tableExists(tableName));

    startTailer(conf);
    createOps();
    stopTailer();

    assertTrue(admin.tableExists(tableName));

    HTable table = new HTable(conf, tableName);
    assertEquals(2, getCount(table));

    int[] values = getValues(table, "a");
    assertEquals(2, values.length);
    assertEquals(4, values[0]);
    assertEquals(1, values[1]);

    assertEquals(2, getSingleValue(table, "b"));

    values = getValues(table, "c");
    assertEquals(0, values.length);

    assertTrue(admin.tableExists("_zw_tailers"));
  }

  @Test
  public void testSkipUpdates() throws Exception {
    Configuration conf = new Configuration(util.getConfiguration());
    ConfigUtil.setSkipBacklog(conf, true);
    ConfigUtil.setSkipUpdates(conf, true);

    HBaseAdmin admin = util.getHBaseAdmin();
    assertFalse(admin.tableExists(tableName));

    startTailer(conf);
    createOps();
    stopTailer();

    assertTrue(admin.tableExists(tableName));

    HTable table = new HTable(conf, tableName);
    assertEquals(2, getCount(table));

    assertEquals(1, getSingleValue(table, "a"));
    assertEquals(2, getSingleValue(table, "b"));

    int[] values = getValues(table, "c");
    assertEquals(0, values.length);
  }

  @Test
  public void testSkipDeletes() throws Exception {
    Configuration conf = new Configuration(util.getConfiguration());
    ConfigUtil.setSkipBacklog(conf, true);
    ConfigUtil.setSkipDeletes(conf, true);

    HBaseAdmin admin = util.getHBaseAdmin();
    assertFalse(admin.tableExists(tableName));

    startTailer(conf);
    createOps();
    stopTailer();

    assertTrue(admin.tableExists(tableName));

    HTable table = new HTable(conf, tableName);
    assertEquals(3, getCount(table));

    int[] values = getValues(table, "a");
    assertEquals(2, values.length);
    assertEquals(4, values[0]);
    assertEquals(1, values[1]);

    assertEquals(2, getSingleValue(table, "b"));
    assertEquals(3, getSingleValue(table, "c"));
  }

  @Test
  public void testInsertOnlyWithBufferedWrites() throws Exception {
    Configuration conf = new Configuration(util.getConfiguration());
    ConfigUtil.setSkipBacklog(conf, true);
    ConfigUtil.setSkipUpdates(conf, true);
    ConfigUtil.setSkipDeletes(conf, true);
    ConfigUtil.setBufferWrites(conf, true);

    HBaseAdmin admin = util.getHBaseAdmin();
    assertFalse(admin.tableExists(tableName));

    startTailer(conf);
    DBCollection collection = mongo.getDB("_test_zerowing").getCollection(name);
    collection.insert(new BasicDBObject("_id", "a").append("num", 1));
    collection.insert(new BasicDBObject("_id", "b").append("num", 2));
    collection.insert(new BasicDBObject("_id", "c").append("num", 3));
    Thread.sleep(15000);
    stopTailer();

    assertTrue(admin.tableExists(tableName));

    HTable table = new HTable(conf, tableName);
    assertEquals(3, getCount(table));

    assertEquals(1, getSingleValue(table, "a"));
    assertEquals(2, getSingleValue(table, "b"));
    assertEquals(3, getSingleValue(table, "c"));
  }

  @Test
  public void testResyncUpdate() throws Exception {
    Configuration conf = new Configuration(util.getConfiguration());
    ConfigUtil.setSkipBacklog(conf, true);

    HBaseAdmin admin = util.getHBaseAdmin();
    assertFalse(admin.tableExists(tableName));

    startTailer(conf);
    DBCollection collection = mongo.getDB("_test_zerowing").getCollection(name);
    collection.insert(new BasicDBObject("_id", "a").append("num", 1));
    collection.update(
      new BasicDBObject("_id", "a"),
      new BasicDBObject("$set", new BasicDBObject("num", 2)));
    Thread.sleep(15000);
    stopTailer();

    assertTrue(admin.tableExists(tableName));

    HTable table = new HTable(conf, tableName);
    assertEquals(1, getCount(table));

    int[] values = getValues(table, "a");
    assertEquals(2, values.length);
    assertEquals(2, values[0]);
    assertEquals(1, values[1]);
  }

  private void createOps() throws Exception {
    DBCollection collection = mongo.getDB("_test_zerowing").getCollection(name);
    collection.insert(new BasicDBObject("_id", "a").append("num", 1));
    collection.insert(new BasicDBObject("_id", "b").append("num", 2));
    collection.insert(new BasicDBObject("_id", "c").append("num", 3));
    collection.update(new BasicDBObject("_id", "a"), new BasicDBObject("num", 4));
    collection.remove(new BasicDBObject("_id", "c"));
    Thread.sleep(15000);
  }

  private static class TailerThread extends Thread {
    Tailer tailer;

    TailerThread(Configuration conf) throws Exception {
      HConnection hbase = HConnectionManager.createConnection(conf);
      tailer = new Tailer(conf, mongo, hbase);
    }

    @Override
    public void run() {
      tailer.tail();
    }
  }

  private void startTailer(Configuration conf) throws Exception {
    tailerThread = new TailerThread(conf);
    tailerThread.start();
    Thread.sleep(2000);
  }

  private void stopTailer() throws Exception {
    tailerThread.interrupt();
    tailerThread.join();
  }

  private int getSingleValue(HTable table, String id) throws Exception {
    int[] values = getValues(table, id);
    assertEquals(1, values.length);
    return values[0];
  }

  private int[] getValues(HTable table, String id) throws Exception {
    BSONDecoder decoder = new BasicBSONDecoder();
    byte[] raw = DigestUtils.md5(id.getBytes());
    Get get = new Get(raw);
    get.setMaxVersions();
    List<KeyValue> kvs = table.get(get).getColumn("z".getBytes(), "w".getBytes());

    int[] values = new int[kvs.size()];
    for (int i = 0; i < kvs.size(); i++) {
      values[i] = (Integer) decoder.readObject(kvs.get(i).getValue()).get("num");
    }

    return values;
  }

  private int getCount(HTable table) throws Exception {
    int count = 0;
    ResultScanner scanner = table.getScanner(new Scan());
    for (Result rs = scanner.next(); rs != null; rs = scanner.next()) { count++; }

    return count;
  }
}
