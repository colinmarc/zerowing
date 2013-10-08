package com.stripe.zerowing;

import java.io.IOException;
import java.util.UUID;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

import com.mongodb.MongoURI;
import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.util.MongoConfigUtil;

public class BulkImportJob {
  private final Job _job;
  private final String _uuid;

  private final MongoURI _mongoURI;
  private final Path _hfilePath;
  private final String _tableName;

  public BulkImportJob(Configuration conf, MongoURI mongoURI, String tableName) throws IOException {
    Configuration cloned = new Configuration(conf);
    _uuid = UUID.randomUUID().toString();
    _job = new Job(cloned, tableName + "." + _uuid);

    _mongoURI = mongoURI;
    _tableName = tableName;

    String tmpPath = ConfigUtil.getTemporaryHFilePath(conf);
    _hfilePath = new Path(tmpPath, _uuid);

    setupJob();
  }

  public Configuration getConfiguration() {
    return _job.getConfiguration();
  }

  public Job getJob() {
    return _job;
  }

  public MongoURI getMongoURI() {
    return _mongoURI;
  }

  public String getOutputTableName() {
    return _tableName;
  }

  public Path getHFilePath() {
    return _hfilePath;
  }

  public boolean run() throws Exception {
    configureBulkImport();

    boolean success = _job.waitForCompletion(true);

    if (success) {
      completeImport();
    }

    return success;
  }

  public void submit() throws Exception {
    configureBulkImport();
    _job.submit();
  }

  public void completeImport() throws Exception {
    LoadIncrementalHFiles loader = new LoadIncrementalHFiles(getConfiguration());
    HTable table = new HTable(getConfiguration(), _tableName);
    loader.doBulkLoad(_hfilePath, table);

    FileSystem fs = _hfilePath.getFileSystem(getConfiguration());
    fs.delete(_hfilePath, true);
  }

  private void setupJob() {
    _job.setInputFormatClass(MongoInputFormat.class);
    _job.setMapperClass(BulkImportMapper.class);
    _job.setMapOutputKeyClass(ImmutableBytesWritable.class);
    _job.setMapOutputValueClass(Put.class);

    MongoConfigUtil.setInputURI(getConfiguration(), _mongoURI);
    MongoConfigUtil.setReadSplitsFromSecondary(getConfiguration(), true);
  }

  private void configureBulkImport() throws IOException {
    HTable table = new HTable(getConfiguration(), _tableName);
    HFileOutputFormat.configureIncrementalLoad(_job, table);
    HFileOutputFormat.setOutputPath(_job, _hfilePath);
  }
}
