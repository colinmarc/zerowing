package com.stripe.zerowing;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;

public class ArchivalTranslator extends BasicTranslator implements Translator{
  @Override
  public HTableDescriptor describeHBaseTable(String tableName) {
    HTableDescriptor tableDesc = new HTableDescriptor(tableName);
    HColumnDescriptor familyDesc = new HColumnDescriptor(COL_FAMILY);
    familyDesc.setMaxVersions(Integer.MAX_VALUE);
    tableDesc.addFamily(familyDesc);

    return tableDesc;
  }
}
