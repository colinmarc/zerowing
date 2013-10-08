package com.stripe.zerowing;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Put;
import org.bson.BSONObject;
import org.bson.BasicBSONEncoder;
import org.bson.types.ObjectId;

public class BasicTranslator implements Translator {
  public final byte[] COL_FAMILY = "z".getBytes();
  public final byte[] COL_QUALIFIER = "w".getBytes();
  public final BasicBSONEncoder bsonEncoder = new BasicBSONEncoder();

  @Override
  public HTableDescriptor describeHBaseTable(String tableName) {
    HTableDescriptor tableDesc = new HTableDescriptor(tableName);
    HColumnDescriptor familyDesc = new HColumnDescriptor(COL_FAMILY);
    tableDesc.addFamily(familyDesc);

    return tableDesc;
  }

  @Override
  public String mapNamespaceToHBaseTable(String database, String collection) {
    return "zw." + database + "." + collection;
  }

  @Override
  public byte[] createRowKey(BSONObject object) {
    Object id = object.get("_id");

    byte[] raw;
    if (id instanceof ObjectId) {
      raw = ((ObjectId) id).toByteArray();
    } else if (id instanceof String) {
      raw = ((String) id).getBytes();
    } else if (id instanceof BSONObject) {
      raw = bsonEncoder.encode(((BSONObject) id));
    } else {
      throw new RuntimeException("Don't know how to serialize _id: " + id.toString());
    }

    return DigestUtils.md5(raw);
  }

  @Override
  public Put createPut(byte[] row, BSONObject object) {
    byte[] raw = bsonEncoder.encode(object);
    Put put = new Put(row);
    put.add(COL_FAMILY, COL_QUALIFIER, raw);

    return put;
  }
}
