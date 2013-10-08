package com.stripe.zerowing;

import java.util.List;

import com.mongodb.*;

import org.bson.*;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;

import org.junit.*;
import static org.junit.Assert.*;
import static org.junit.Assume.*;

public class TestBulkImport {

  static HBaseTestingUtility util = new HBaseTestingUtility();
  static MongoClient mongo;

  @BeforeClass
  public static void setUpClass() throws Exception {
    try {
      mongo = new MongoClient("localhost", 27017);
      mongo.getDatabaseNames();
    } catch (Exception e) {
      System.out.println("error connecting to mongo: " + e);
      assumeTrue(false);
    }

    util.startMiniCluster();
    util.startMiniMapReduceCluster();
  }

  @Before
  public void setUp() throws Exception {
    try {
      mongo.getDB("_test_zerowing").dropDatabase();

      DBCollection foo = mongo.getDB("_test_zerowing").getCollection("foo");
      foo.insert(new BasicDBObject("_id", "a").append("num", 1));
      foo.insert(new BasicDBObject("_id", "b").append("num", 2));
      foo.insert(new BasicDBObject("_id", "c").append("num", 3));

      DBCollection bar = mongo.getDB("_test_zerowing").getCollection("bar");
      bar.insert(new BasicDBObject("_id", "a").append("num", 11));
      bar.insert(new BasicDBObject("_id", "b").append("num", 12));
      bar.insert(new BasicDBObject("_id", "c").append("num", 13));
    } catch(Exception e) {
      System.out.println("error setting up mongo: " + e);
    }
  }

  @After
  public void tearDown() throws Exception {
     try {
       HBaseAdmin hbase = util.getHBaseAdmin();
       hbase.disableTable("zw._test_zerowing.foo");
       hbase.deleteTable("zw._test_zerowing.foo");
     } catch (TableNotFoundException e) {}

     try {
       HBaseAdmin hbase = util.getHBaseAdmin();
       hbase.disableTable("zw._test_zerowing.bar");
       hbase.deleteTable("zw._test_zerowing.bar");
     } catch (TableNotFoundException e) {}
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    util.shutdownMiniMapReduceCluster();
    util.shutdownMiniCluster();
  }

  @Test
  public void testBasicImport() throws Exception {
    Configuration conf = util.getConfiguration();
    BulkImportRunner runner = new BulkImportRunner(conf);
    runner.addJob("mongodb://localhost:27017/_test_zerowing.foo");
    assertTrue(runner.doBulkImport());

    HBaseAdmin admin = util.getHBaseAdmin();
    assertTrue(admin.tableExists("zw._test_zerowing.foo"));
    assertFalse(admin.tableExists("zw._test_zerowing.bar"));

    HTable table = new HTable(conf, "zw._test_zerowing.foo");
    assertEquals(3, getCount(table));
    assertEquals(1, getSingleValue(table, "a"));
    assertEquals(2, getSingleValue(table, "b"));
    assertEquals(3, getSingleValue(table, "c"));
  }

  @Test
  public void testExpandDatabase() throws Exception {
    Configuration conf = util.getConfiguration();

    BulkImportRunner runner = new BulkImportRunner(conf);
    runner.addJobsForNamespace("mongodb://localhost:27017/", "_test_zerowing", null);
    assertTrue(runner.doBulkImport());

    HBaseAdmin admin = util.getHBaseAdmin();
    assertTrue(admin.tableExists("zw._test_zerowing.foo"));
    assertTrue(admin.tableExists("zw._test_zerowing.bar"));
  }

  @Test
  public void testMerge() throws Exception {
    Configuration conf = util.getConfiguration();

    BulkImportRunner runner = new BulkImportRunner(conf);
    runner.addJob("mongodb://localhost:27017/_test_zerowing.foo");
    assertTrue(runner.doBulkImport());

    HTable table = new HTable(conf, "zw._test_zerowing.foo");
    assertEquals(3, getCount(table));
    assertEquals(1, getSingleValue(table, "a"));
    assertEquals(2, getSingleValue(table, "b"));
    assertEquals(3, getSingleValue(table, "c"));

    DBCollection foo = mongo.getDB("_test_zerowing").getCollection("foo");
    foo.insert(new BasicDBObject("_id", "d").append("num", 4));
    foo.insert(new BasicDBObject("_id", "e").append("num", 5));
    foo.update(new BasicDBObject("_id", "a"), new BasicDBObject("num", 6));

    ConfigUtil.setMergeExistingTable(conf, true);
    runner = new BulkImportRunner(conf);
    runner.addJob("mongodb://localhost:27017/_test_zerowing.foo");
    assertTrue(runner.doBulkImport());

    //a should have the old value stored under the new one
    int[] values = getValues(table, "a");
    assertEquals(2, values.length);
    assertEquals(6, values[0]);
    assertEquals(1, values[1]);

    values = getValues(table, "b");
    assertEquals(2, values.length);
    assertEquals(2, values[0]);
    assertEquals(2, values[1]);

    values = getValues(table, "c");
    assertEquals(2, values.length);
    assertEquals(3, values[0]);
    assertEquals(3, values[1]);

    assertEquals(4, getSingleValue(table, "d"));
    assertEquals(5, getSingleValue(table, "e"));

    assertEquals(5, getCount(table));
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
