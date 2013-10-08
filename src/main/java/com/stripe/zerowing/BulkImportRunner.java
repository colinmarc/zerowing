package com.stripe.zerowing;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.List;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Set;

import javax.ws.rs.core.UriBuilder;

import org.apache.commons.lang.ClassUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.RegionSplitter;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.mongodb.DBCollection;
import com.mongodb.Mongo;
import com.mongodb.MongoURI;
import com.mongodb.hadoop.util.MongoConfigUtil;

public class BulkImportRunner implements Tool {
  private static final Log log = LogFactory.getLog(BulkImportRunner.class);

  private final LinkedList<BulkImportJob> _jobs = new LinkedList<BulkImportJob>();

  private Configuration _conf;
  private HBaseAdmin _hbaseAdmin;
  private Translator _translator;

  public BulkImportRunner() {
    this(HBaseConfiguration.create());
  }

  public BulkImportRunner(Configuration conf) {
    _conf = conf;
  }

  @Override
  public Configuration getConf() {
    return _conf;
  }

  @Override
  public void setConf(Configuration conf) {
    _conf = conf;
  }

  public HBaseAdmin getHBaseAdmin() {
    if(_hbaseAdmin != null) return _hbaseAdmin;

    try {
      _hbaseAdmin = new HBaseAdmin(_conf);
    } catch (Exception e) {
      throw new RuntimeException("Error connecting to HBase", e);
    }

    return _hbaseAdmin;
  }

  public Translator getTranslator() {
    if(_translator != null) return _translator;

    _translator = ConfigUtil.getTranslator(_conf);
    return _translator;
  }

  public void addJob(String mongoURI) {
    @SuppressWarnings("deprecation")
    MongoURI uri = new MongoURI(mongoURI);

    String database = uri.getDatabase(), collection = uri.getCollection();
    String tableName = getTranslator().mapNamespaceToHBaseTable(database, collection);

    if (tableName == null) {
      log.info("Skipping namespace '" + database + "." + collection + "' because it doesn't map to an HBase table");
      return;
    }

    addJob(uri, tableName);
  }

  public void addJobsForNamespace(String rawURI, String database, String collection) {
    Mongo mongo;

    @SuppressWarnings("deprecation")
    MongoURI uri = new MongoURI(rawURI);

    try {
      mongo = uri.connect();
    } catch (UnknownHostException e) {
      throw new IllegalArgumentException("Failed to connect to MongoDB using URI: " + uri, e);
    }

    if (database != null && collection != null) {
      if(!mongo.getDB(database).collectionExists(collection)) {
        throw new IllegalArgumentException("Couldn't find namespace: " + database + "." + collection);
      }

      buildJob(uri, database, collection);
      return;
    } else if (database == null && collection != null) {
      throw new IllegalArgumentException("You can't specify a MongoDB collection without also specifying a database");
    }

    List<String> databases;
    if (database != null) {
      databases = new ArrayList<String>(1);
      databases.add(database);
    } else {
      databases = mongo.getDatabaseNames();
    }

    for (String db : databases) {
      if (db.equals("local") || db.equals("admin")) continue;

      Set<String> collections = mongo.getDB(db).getCollectionNames();
      for (String coll : collections) {
        if (coll.startsWith("system.")) continue;
        buildJob(uri, db, coll);
      }
    }
  }

  public boolean doBulkImport() throws Exception{
    if (!createTables()) return false;

    for (BulkImportJob job : _jobs) {
      try {
        job.submit();
      } catch (Exception e) {
        log.error("Error while running job", e);
        return false;
      }
    }

    boolean success = true;
    for (BulkImportJob job : _jobs) {
      job.getJob().waitForCompletion(true);

      if (job.getJob().isSuccessful()) {
        job.completeImport();
      } else {
        success = false;
      }
    }

    return true;
  }

  private boolean createTables() throws Exception {
    Translator translator = getTranslator();
    HBaseAdmin admin = getHBaseAdmin();
    HashSet<String> createdTables = new HashSet<String>();

    for (BulkImportJob job : _jobs) {
      String tableName = job.getOutputTableName();

      if (!createdTables.contains(tableName)) {
        if (admin.tableExists(tableName)) {
          if(ConfigUtil.getMergeExistingTable(_conf)) {
            continue;
          } else {
            log.error("Table already exists: " + tableName + ". If you want to merge with an existing table, use the --merge option");
            return false;
          }
        }

        HTableDescriptor tableDesc = translator.describeHBaseTable(tableName);

        try {
          if(ConfigUtil.getPresplitTable(_conf)) {
            byte[][] splits = calculateRegionSplits(job.getMongoURI(), tableName);
            admin.createTable(tableDesc, splits);
          } else {
            admin.createTable(tableDesc);
          }
        } catch (IOException e) {
          log.error("Failed to create table: " + tableName, e);
          return false;
        }

        createdTables.add(tableName);
      }
    }

    return true;
  }

  private byte[][] calculateRegionSplits(MongoURI uri, String tableName) throws Exception {
    DBCollection collection = uri.connectDB().getCollection(uri.getCollection());
    long size = collection.getStats().getLong("size");
    int regionSize = ConfigUtil.getPresplitTableRegionSize(_conf);
    int numRegions = (int) Math.min((size / regionSize) + 1, 4096);

    if (numRegions > 1) {
      log.info("Pre-splitting " + tableName + " into " + numRegions + " regions");
      RegionSplitter.UniformSplit splitter = new RegionSplitter.UniformSplit();
      return splitter.split(numRegions);
    } else {
      log.info("Not splitting " + tableName + ", because the data can fit into a single region");
      return new byte[0][0];
    }
  }

  private void buildJob(MongoURI uri, String database, String collection) {
    UriBuilder builder = UriBuilder.fromUri(uri.toString());
    String builtURI = builder.path("{db}.{coll}").build(database, collection).toString();

    addJob(builtURI);
  }

  private void addJob(MongoURI uri, String tableName) {
    log.info("Adding a job for " + uri + " -> " + tableName);

    try {
        BulkImportJob job = new BulkImportJob(_conf, uri, tableName);
        job.getJob().setJarByClass(BulkImportRunner.class); //TODO
        _jobs.add(job);
      } catch (IOException e) {
        log.error("Failed to add job", e);
    }
  }

  private static class CommandLineOptions {
    @Parameter(names = {"-m", "--mongo"}, description = "The base MongoDB URI to connect to", required = true)
    private String mongoURI;

    @Parameter(names = {"-d", "--database"}, description = "The MongoDB database to import")
    private String mongoDatabase;

    @Parameter(names = {"-c", "--collection"}, description = "The MongoDB collection to import")
    private String mongoCollection;

    @Parameter(names = {"-t", "--translator"}, description = "Specify a ZWTranslator class to use. Defaults to com.stripe.zerowing.ZWBasicTranslator")
    private String translatorClass;

    @Parameter(names = "--merge", description = "Copy into existing tables. Unless this option is set, the import will fail if any tables already exists")
    private boolean merge;

    @Parameter(names = "--tmp-dir", description = "The temporary HDFS directory to write HFiles to (will be created). Defaults to /tmp/zerowing_hfile_out")
    private String tmpDir;

    @Parameter(names = "--no-split", description = "Don't presplit HBase tables")
    private boolean noSplit;

    @Parameter(names = "--region-size", description = "Size of regions to presplit HBase tables into. Defaults to 512mb")
    private String regionSize;

    @Parameter(names = "--mongo-split-size", description = "Size of chunks to split Mongo collections into. Defaults to 8mb")
    private String mongoSplitSize;

    @Parameter(names = "--help", help = true, description = "Show this message")
    private boolean help;
  }

  @Override
  public int run(String[] args) throws Exception {
    String[] remainingArgs = new GenericOptionsParser(args).getRemainingArgs();
    CommandLineOptions opts = new CommandLineOptions();
    JCommander parser = new JCommander(opts, remainingArgs);

    if (opts.help) {
      parser.usage();
      return 0;
    }

    if (opts.translatorClass != null) {
      log.info("Setting the translator class to " + opts.translatorClass);

      try {
        @SuppressWarnings("unchecked")
        Class<? extends Translator> translatorClass = ClassUtils.getClass(opts.translatorClass);
        ConfigUtil.setTranslatorClass(_conf, translatorClass);
      } catch (ClassNotFoundException e) {
        log.error("Couldn't find translator class: " + opts.translatorClass, e);
        return 1;
      }
    }

    if (opts.tmpDir != null) {
      log.info("Using " + opts.tmpDir + " as the temporary HFile path");
      ConfigUtil.setTemporaryHFilePath(_conf, opts.tmpDir);
    }

    if (opts.noSplit) {
      ConfigUtil.setPresplitTable(_conf, false);
    }

    if (opts.merge) {
      ConfigUtil.setMergeExistingTable(_conf, true);
    }

    if (opts.regionSize != null) {
      ConfigUtil.setPresplitTableRegionSize(_conf, Integer.parseInt(opts.regionSize));
    }

    if (opts.mongoSplitSize != null) {
      MongoConfigUtil.setSplitSize(_conf, Integer.parseInt(opts.mongoSplitSize));
    }

    @SuppressWarnings("deprecation")
    MongoURI uri = new MongoURI(opts.mongoURI);

    String database = null;
    if (opts.mongoDatabase != null) {
      database = opts.mongoDatabase;
    } else if (uri.getDatabase() != null) {
      database = uri.getDatabase();
    }

    String collection = null;
    if (opts.mongoCollection != null) {
      collection = opts.mongoCollection;
    } else if (uri.getCollection() != null) {
      collection = uri.getCollection();
    }

    addJobsForNamespace(opts.mongoURI, database, collection);

    if (!doBulkImport()) {
      return 1;
    }

    return 0;
  }

  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new BulkImportRunner(), args));
  }
}
