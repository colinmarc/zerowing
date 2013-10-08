package com.stripe.zerowing;

import org.apache.hadoop.conf.Configuration;

public class ConfigUtil {
  public static final String TRANSLATOR_CLASS = "zerowing.translator_class";
  public static final String TEMPORARY_HFILE_PATH = "zerowing.import.tmp_hfile_path";
  public static final String SHOULD_PRESPLIT_TABLE = "zerowing.import.presplit_table";
  public static final String PRESPLIT_TABLE_REGION_SIZE = "zerowing.import.resplit_table_chunk_size";
  public static final String MERGE_EXISTING_TABLE = "zerowing.import.merge_existing";
  public static final String SKIP_BACKLOG = "zerowing.tailer.skip_backlog";
  public static final String SKIP_UPDATES = "zerowing.tailer.skip_updates";
  public static final String SKIP_DELETES = "zerowing.tailer.skip_deletes";
  public static final String TAILER_STATE_TABLE = "zerowing.tailer.state_table";

  public static final String DEFAULT_TEMPORARY_HFILE_PATH = "/tmp/zerowing_hfile_out";
  public static final int DEFAULT_PRESPLIT_TABLE_REGION_SIZE = 536870912; //512mb
  public static final String DEFAULT_TAILER_STATE_TABLE = "_zw_tailers";

  public static void setTranslatorClass(Configuration conf, Class<? extends Translator> translatorClass) {
    conf.setClass(TRANSLATOR_CLASS, translatorClass, Translator.class);
  }

  public static Class<? extends Translator> getTranslatorClass(Configuration conf) {
    return conf.getClass(TRANSLATOR_CLASS, BasicTranslator.class, Translator.class);
  }

  public static Translator getTranslator(Configuration conf) {
    Class<? extends Translator> translatorClass = ConfigUtil.getTranslatorClass(conf);

    try {
      return translatorClass.newInstance();
    } catch (Exception e) {
      throw new RuntimeException("Error instantiating ZWTranslator class", e);
    }
  }

  public static void setTemporaryHFilePath(Configuration conf, String hfilePath) {
    conf.set(TEMPORARY_HFILE_PATH, hfilePath);
  }

  public static String getTemporaryHFilePath(Configuration conf) {
    return conf.get(TEMPORARY_HFILE_PATH, DEFAULT_TEMPORARY_HFILE_PATH);
  }

  public static void setPresplitTable(Configuration conf, boolean presplitTable) {
    conf.setBoolean(SHOULD_PRESPLIT_TABLE, presplitTable);
  }

  public static boolean getPresplitTable(Configuration conf) {
    return conf.getBoolean(SHOULD_PRESPLIT_TABLE, true);
  }

  public static void setPresplitTableRegionSize(Configuration conf, int numBytes) {
    conf.setInt(PRESPLIT_TABLE_REGION_SIZE, numBytes);
  }

  public static int getPresplitTableRegionSize(Configuration conf) {
    return conf.getInt(PRESPLIT_TABLE_REGION_SIZE, DEFAULT_PRESPLIT_TABLE_REGION_SIZE);
  }

  public static void setMergeExistingTable(Configuration conf, boolean mergeExisting) {
    conf.setBoolean(MERGE_EXISTING_TABLE, mergeExisting);
  }

  public static boolean getMergeExistingTable(Configuration conf) {
    return conf.getBoolean(MERGE_EXISTING_TABLE, false);
  }

  public static void setSkipBacklog(Configuration conf, boolean skipBacklog) {
    conf.setBoolean(SKIP_BACKLOG, skipBacklog);
  }

  public static boolean getSkipBacklog(Configuration conf) {
    return conf.getBoolean(SKIP_BACKLOG, false);
  }

  public static void setSkipUpdates(Configuration conf, boolean skipUpdates) {
    conf.setBoolean(SKIP_UPDATES, skipUpdates);
  }

  public static boolean getSkipUpdates(Configuration conf) {
    return conf.getBoolean(SKIP_UPDATES, false);
  }

  public static void setSkipDeletes(Configuration conf, boolean skipDeletes) {
    conf.setBoolean(SKIP_DELETES, skipDeletes);
  }

  public static boolean getSkipDeletes(Configuration conf) {
    return conf.getBoolean(SKIP_DELETES, false);
  }

  public static void setTailerStateTable(Configuration conf, String stateTableName) {
    conf.set(TAILER_STATE_TABLE, stateTableName);
  }

  public static String getTailerStateTable(Configuration conf) {
    return conf.get(TAILER_STATE_TABLE, DEFAULT_TAILER_STATE_TABLE);
  }
}
