/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.catalog.store;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hcatalog.common.HCatUtil;
import org.apache.hcatalog.data.Pair;
import org.apache.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hcatalog.data.schema.HCatSchema;
import org.apache.tajo.catalog.*;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.exception.CatalogException;
import org.apache.tajo.catalog.partition.PartitionMethodDesc;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.exception.InternalException;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import static org.apache.tajo.catalog.proto.CatalogProtos.PartitionType;

public class HCatalogStore extends CatalogConstants implements CatalogStore {
  public static final String CSVFILE_DELIMITER = "csvfile.delimiter";
  public static final String CSVFILE_NULL = "csvfile.null";

  protected final Log LOG = LogFactory.getLog(getClass());
  protected Configuration conf;
  private static final int CLIENT_POOL_SIZE = 2;
  private final HCatalogStoreClientPool clientPool = new HCatalogStoreClientPool(0);
  private ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();

  public HCatalogStore(final Configuration conf)
      throws InternalException {
    this.conf = conf;
    try {
      // In unit test case, HCatalogStore has to set some hive configurations by force.
      // So, it checks caller class name and caller method name.
      long[] threadIds = threadBean.getAllThreadIds();
      for (long tid : threadIds) {
        ThreadInfo info = threadBean.getThreadInfo(tid, 5);
        for (StackTraceElement frame : info.getStackTrace()) {
          if (frame.toString().equals("org.apache.tajo.catalog.store.TestHCatalogStore.setUp")) {
            clientPool.setParameters(this.conf);
          }
        }
      }

      clientPool.addClients(CLIENT_POOL_SIZE);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Override
  public boolean existTable(final String name) throws CatalogException {
    boolean exist = false;

    String dbName = null, tableName = null;
    Pair<String, String> tablePair = null;
    org.apache.hadoop.hive.ql.metadata.Table table = null;
    HCatalogStoreClientPool.HCatalogStoreClient client = null;
    // get db name and table name.
    try {
      tablePair = HCatUtil.getDbAndTableName(name);
      dbName = tablePair.first;
      tableName = tablePair.second;
    } catch (Exception ioe) {
      throw new CatalogException("Table name is wrong.", ioe);
    }

    // get table
    try {
      try {
        client = clientPool.getClient();
        table = HCatUtil.getTable(client.getHiveClient(), dbName, tableName);
        if (table != null) {
          exist = true;
        }
      } catch (NoSuchObjectException nsoe) {
        exist = false;
      } catch (Exception e) {
        throw new CatalogException(e);
      }
    } finally {
      client.release();
    }

    return exist;
  }

  @Override
  public final CatalogProtos.TableDescProto getTable(final String name) throws CatalogException {
    String dbName = null, tableName = null;
    Pair<String, String> tablePair = null;
    org.apache.hadoop.hive.ql.metadata.Table table = null;
    HCatalogStoreClientPool.HCatalogStoreClient client = null;
    Path path = null;
    CatalogProtos.StoreType storeType = null;
    org.apache.tajo.catalog.Schema schema = null;
    Options options = null;
    TableStats stats = null;
    PartitionMethodDesc partitions = null;

    // get db name and table name.
    try {
      tablePair = HCatUtil.getDbAndTableName(name);
      dbName = tablePair.first;
      tableName = tablePair.second;
    } catch (Exception ioe) {
      throw new CatalogException("Table name is wrong.", ioe);
    }

    //////////////////////////////////
    // set tajo table schema.
    //////////////////////////////////
    try {
      // get hive table schema
      try {
        client = clientPool.getClient();
        table = HCatUtil.getTable(client.getHiveClient(), dbName, tableName);
        path = table.getPath();
      } catch (NoSuchObjectException nsoe) {
        throw new CatalogException("Table not found. - tableName:" + name, nsoe);
      } catch (Exception e) {
        throw new CatalogException(e);
      }

      // convert hcatalog field schema into tajo field schema.
      schema = new org.apache.tajo.catalog.Schema();
      HCatSchema tableSchema = null;

      try {
        tableSchema = HCatUtil.getTableSchemaWithPtnCols(table);
      } catch (IOException ioe) {
        throw new CatalogException("Fail to get table schema. - tableName:" + name, ioe);
      }
      List<HCatFieldSchema> fieldSchemaList = tableSchema.getFields();
      boolean isPartitionKey = false;
      for (HCatFieldSchema eachField : fieldSchemaList) {
        isPartitionKey = false;

        if (table.getPartitionKeys() != null) {
          for(FieldSchema partitionKey: table.getPartitionKeys()) {
            if (partitionKey.getName().equals(eachField.getName())) {
              isPartitionKey = true;
            }
          }
        }

        if (!isPartitionKey) {
          String fieldName = dbName + "." + tableName + "." + eachField.getName();
          TajoDataTypes.Type dataType = HCatalogUtil.getTajoFieldType(eachField.getType().toString());
          schema.addColumn(fieldName, dataType);
        }
      }

      // validate field schema.
      try {
        HCatalogUtil.validateHCatTableAndTajoSchema(tableSchema);
      } catch (Exception e) {
        throw new CatalogException("HCatalog cannot support schema. - schema:" + tableSchema.toString(), e);
      }

      stats = new TableStats();
      options = Options.create();
      Properties properties = table.getMetadata();
      if (properties != null) {
        // set field delimiter
        String fieldDelimiter = "", fileOutputformat = "", nullFormat = "";
        if (properties.getProperty(serdeConstants.FIELD_DELIM) != null) {
          fieldDelimiter = properties.getProperty(serdeConstants.FIELD_DELIM);
        } else {
          // if hive table used default row format delimiter, Properties doesn't have it.
          // So, Tajo must set as follows:
          fieldDelimiter = "\u0001";
        }

        // set null format
        if (properties.getProperty(serdeConstants.SERIALIZATION_NULL_FORMAT) != null) {
          nullFormat = properties.getProperty(serdeConstants.SERIALIZATION_NULL_FORMAT);
        } else {
          nullFormat = "\\N";
        }

        // set file output format
        fileOutputformat = properties.getProperty("file.outputformat");
        storeType = CatalogUtil.getStoreType(HCatalogUtil.getStoreType(fileOutputformat));
        if (storeType.equals(CatalogProtos.StoreType.CSV) ) {
          options.put(CSVFILE_DELIMITER, StringEscapeUtils.escapeJava(fieldDelimiter));
          options.put(CSVFILE_NULL, StringEscapeUtils.escapeJava(nullFormat));
        }

        // set data size
        long totalSize = 0;
        if(properties.getProperty("totalSize") != null) {
          totalSize = new Long(properties.getProperty("totalSize"));
        } else {
          try {
            FileSystem fs = path.getFileSystem(conf);
            if (fs.exists(path)) {
              totalSize = fs.getContentSummary(path).getLength();
            }
          } catch (IOException ioe) {
            throw new CatalogException("Fail to get path. - path:" + path.toString(), ioe);
          }
        }
        stats.setNumBytes(totalSize);
      }

      // set partition keys
      if (table.getPartitionKeys() != null) {
        Schema expressionSchema = new Schema();
        StringBuilder sb = new StringBuilder();
        if (table.getPartitionKeys().size() > 0) {
          List<FieldSchema> partitionKeys = table.getPartitionKeys();
          for(int i = 0; i < partitionKeys.size(); i++) {
            FieldSchema fieldSchema = partitionKeys.get(i);
            TajoDataTypes.Type dataType = HCatalogUtil.getTajoFieldType(fieldSchema.getType().toString());
            expressionSchema.addColumn(new Column(dbName + "." + tableName + "." + fieldSchema.getName(), dataType));
            if (i > 0) {
              sb.append(",");
            }
            sb.append(fieldSchema.getName());
          }
          partitions = new PartitionMethodDesc(
              tableName,
              PartitionType.COLUMN,
              sb.toString(),
              expressionSchema);
        }
      }
    } finally {
      client.release();
    }
    TableMeta meta = new TableMeta(storeType, options);

    TableDesc tableDesc = new TableDesc(dbName + "." + tableName, schema, meta, path);
    if (stats != null) {
      tableDesc.setStats(stats);
    }
    if (partitions != null) {
      tableDesc.setPartitionMethod(partitions);
    }
    return tableDesc.getProto();
  }


  private TajoDataTypes.Type getDataType(final String typeStr) {
    try {
      return Enum.valueOf(TajoDataTypes.Type.class, typeStr);
    } catch (IllegalArgumentException iae) {
      LOG.error("Cannot find a matched type aginst from '" + typeStr + "'");
      return null;
    }
  }

  @Override
  public final List<String> getAllTableNames() throws CatalogException {
    List<String> dbs = null;
    List<String> tables = null;
    List<String> allTables = new ArrayList<String>();
    HCatalogStoreClientPool.HCatalogStoreClient client = null;

    try {
      client = clientPool.getClient();
      dbs = client.getHiveClient().getAllDatabases();
      for(String eachDB: dbs) {
        tables = client.getHiveClient().getAllTables(eachDB);
        for(String eachTable: tables) {
          allTables.add(eachDB + "." + eachTable);
        }
      }
    } catch (MetaException e) {
      throw new CatalogException(e);
    } finally {
      client.release();
    }
    return allTables;
  }

  @Override
  public final void addTable(final CatalogProtos.TableDescProto tableDesc) throws CatalogException {
    String dbName = null, tableName = null;
    Pair<String, String> tablePair = null;
    HCatalogStoreClientPool.HCatalogStoreClient client = null;

    // get db name and table name.
    try {
      tablePair = HCatUtil.getDbAndTableName(tableDesc.getId());
      dbName = tablePair.first;
      tableName = tablePair.second;
    } catch (Exception ioe) {
      throw new CatalogException("Table name is wrong.", ioe);
    }

    try {
      client = clientPool.getClient();

      org.apache.hadoop.hive.metastore.api.Table table = new org.apache.hadoop.hive.metastore.api.Table();

      table.setDbName(dbName);
      table.setTableName(tableName);
      // TODO: set owner
      //table.setOwner();

      StorageDescriptor sd = new StorageDescriptor();
      sd.setParameters(new HashMap<String, String>());
      sd.setSerdeInfo(new SerDeInfo());
      sd.getSerdeInfo().setParameters(new HashMap<String, String>());
      sd.getSerdeInfo().setName(table.getTableName());

      // if tajo set location method, thrift client make exception as follows:
      // Caused by: MetaException(message:java.lang.NullPointerException)
      // If you want to modify table path, you have to modify on Hive cli.
      // sd.setLocation(tableDesc.getPath().toString());

      // set column information
      ArrayList<FieldSchema> cols = new ArrayList<FieldSchema>(tableDesc.getSchema().getFieldsCount());

      for (CatalogProtos.ColumnProto eachField : tableDesc.getSchema().getFieldsList()) {
        cols.add(new FieldSchema( eachField.getName(), HCatalogUtil.getHiveFieldType(eachField.getDataType().getType().name()), ""));
      }
      sd.setCols(cols);

      // set partition keys
      if (tableDesc.getPartition() != null && tableDesc.getPartition().getPartitionType().equals(PartitionType.COLUMN)) {
        List<FieldSchema> partitionKeys = new ArrayList<FieldSchema>();
        for(CatalogProtos.ColumnProto eachPartitionKey: tableDesc.getPartition().getExpressionSchema().getFieldsList()) {
          partitionKeys.add(new FieldSchema( eachPartitionKey.getName(), HCatalogUtil.getHiveFieldType(eachPartitionKey.getDataType().getType().name()), ""));
        }
        table.setPartitionKeys(partitionKeys);
      }

      sd.setCompressed(false);

      if (tableDesc.getMeta().hasParams()) {
        for (CatalogProtos.KeyValueProto entry: tableDesc.getMeta().getParams().getKeyvalList()) {
          if (entry.getKey().equals("compression.codec")) {
            sd.setCompressed(true);
          } else if (entry.getKey().equals(CSVFILE_NULL)) {
            sd.getSerdeInfo().getParameters().put(serdeConstants.SERIALIZATION_NULL_FORMAT, StringEscapeUtils.unescapeJava(entry.getValue()));
          } else if (entry.getKey().equals(CSVFILE_DELIMITER)) {
            String fieldDelimiter = entry.getValue();

            // User can use an unicode for filed delimiter such as \u0001, \001.
            // In this case, java console will convert this value into "\\u001".
            // And hive will un-espace this value again.
            // As a result, user can use right field delimiter.
            // So, we have to un-escape this value.
            sd.getSerdeInfo().getParameters().put(serdeConstants.SERIALIZATION_FORMAT, StringEscapeUtils.unescapeJava(fieldDelimiter));
            sd.getSerdeInfo().getParameters().put(serdeConstants.FIELD_DELIM, StringEscapeUtils.unescapeJava(fieldDelimiter));
          }
        }
      }

      if(tableDesc.getMeta().getStoreType().equals(CatalogProtos.StoreType.RCFILE)) {
        sd.getSerdeInfo().setSerializationLib(org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe.class.getName());
      } else {
        sd.getSerdeInfo().setSerializationLib(org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe.class.getName());
      }

      if(tableDesc.getMeta().getStoreType().equals(CatalogProtos.StoreType.RCFILE)) {
        sd.setInputFormat(org.apache.hadoop.hive.ql.io.RCFileInputFormat.class.getName());
      } else {
        sd.setInputFormat(org.apache.hadoop.mapred.TextInputFormat.class.getName());
      }

      if(tableDesc.getMeta().getStoreType().equals(CatalogProtos.StoreType.RCFILE)) {
        sd.setOutputFormat(org.apache.hadoop.hive.ql.io.RCFileOutputFormat.class.getName());
      } else {
        sd.setOutputFormat(org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat.class.getName());
      }

      sd.setSortCols(new ArrayList<Order>());

      table.setSd(sd);
      client.getHiveClient().createTable(table);
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new CatalogException(e);
    } finally {
      client.release();
    }
  }

  @Override
  public final void deleteTable(final String name) throws CatalogException {
    String dbName = null, tableName = null;
    Pair<String, String> tablePair = null;
    HCatalogStoreClientPool.HCatalogStoreClient client = null;

    // get db name and table name.
    try {
      tablePair = HCatUtil.getDbAndTableName(name);
      dbName = tablePair.first;
      tableName = tablePair.second;
    } catch (Exception ioe) {
      throw new CatalogException("Table name is wrong.", ioe);
    }

    try {
      client = clientPool.getClient();
      client.getHiveClient().dropTable(dbName, tableName, false, false);
    } catch (NoSuchObjectException nsoe) {
    } catch (Exception e) {
      throw new CatalogException(e);
    } finally {
      client.release();
    }
  }

  @Override
  public void addPartitionMethod(CatalogProtos.PartitionMethodProto partitionMethodProto) throws CatalogException {
    // TODO - not implemented yet
  }

  @Override
  public CatalogProtos.PartitionMethodProto getPartitionMethod(String tableName) throws CatalogException {
    return null;  // TODO - not implemented yet
  }

  @Override
  public boolean existPartitionMethod(String tableName) throws CatalogException {
    return false;  // TODO - not implemented yet
  }

  @Override
  public void delPartitionMethod(String tableName) throws CatalogException {
    // TODO - not implemented yet
  }

  @Override
  public void addPartitions(CatalogProtos.PartitionsProto partitionsProto) throws CatalogException {
    // TODO - not implemented yet
  }

  @Override
  public void addPartition(CatalogProtos.PartitionDescProto partitionDescProto) throws CatalogException {
    // TODO - not implemented yet
  }

  @Override
  public CatalogProtos.PartitionsProto getPartitions(String tableName) throws CatalogException {
    return null; // TODO - not implemented yet
  }

  @Override
  public CatalogProtos.PartitionDescProto getPartition(String partitionName) throws CatalogException {
    return null; // TODO - not implemented yet
  }

  @Override
  public void delPartition(String partitionName) throws CatalogException {
    // TODO - not implemented yet
  }

  @Override
  public void delPartitions(String tableName) throws CatalogException {
    // TODO - not implemented yet
  }

  @Override
  public final void addFunction(final FunctionDesc func) throws CatalogException {
    // TODO - not implemented yet
  }

  @Override
  public final void deleteFunction(final FunctionDesc func) throws CatalogException {
    // TODO - not implemented yet
  }

  @Override
  public final void existFunction(final FunctionDesc func) throws CatalogException {
    // TODO - not implemented yet
  }

  @Override
  public final List<String> getAllFunctionNames() throws CatalogException {
    // TODO - not implemented yet
    return null;
  }

  @Override
  public void delIndex(String indexName) throws CatalogException {
    // TODO - not implemented yet
  }

  @Override
  public boolean existIndex(String indexName) throws CatalogException {
    // TODO - not implemented yet
    return false;
  }

  @Override
  public CatalogProtos.IndexDescProto[] getIndexes(String tableName) throws CatalogException {
    // TODO - not implemented yet
    return null;
  }

  @Override
  public void addIndex(CatalogProtos.IndexDescProto proto) throws CatalogException {
    // TODO - not implemented yet
  }

  @Override
  public CatalogProtos.IndexDescProto getIndex(String indexName) throws CatalogException {
    // TODO - not implemented yet
    return null;
  }

  @Override
  public CatalogProtos.IndexDescProto getIndex(String tableName, String columnName) throws CatalogException {
    // TODO - not implemented yet
    return null;
  }

  @Override
  public boolean existIndex(String tableName, String columnName) throws CatalogException{
    // TODO - not implemented yet
    return false;
  }

  @Override
  public final void close() {
    clientPool.close();
  }
}
