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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hcatalog.common.HCatUtil;
import org.apache.hcatalog.data.Pair;
import org.apache.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hcatalog.data.schema.HCatSchema;
import org.apache.tajo.catalog.*;
import org.apache.tajo.catalog.partition.PartitionDesc;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.exception.InternalException;

import java.io.IOException;
import java.util.*;

public class HCatalogStore extends CatalogConstants implements CatalogStore {
  protected final Log LOG = LogFactory.getLog(getClass());
  protected Configuration conf;
  protected String catalogUri;
  private Map<Pair<String, String>, Table> tableMap = new HashMap<Pair<String, String>, Table>();

  public HCatalogStore(final Configuration conf)
      throws InternalException {
    this.conf = conf;
    if(conf.get(CatalogConstants.DEPRECATED_CATALOG_URI) != null) {
      LOG.warn("Configuration parameter " + CatalogConstants.DEPRECATED_CATALOG_URI + " " +
          "is deprecated. Use " + CatalogConstants.CATALOG_URI + " instead.");
      this.catalogUri = conf.get(CatalogConstants.DEPRECATED_CATALOG_URI);
    } else {
      this.catalogUri = conf.get(CatalogConstants.CATALOG_URI);
    }
  }

  @Override
  public final boolean existTable(final String name) throws IOException {
    boolean exist = false;

    String dbName = null, tableName = null;
    Pair<String, String> tablePair = null;
    org.apache.hadoop.hive.ql.metadata.Table table = null;
    HiveMetaStoreClient client = null;

    // get db name and table name.
    try {
      tablePair = HCatUtil.getDbAndTableName(name);
      dbName = tablePair.first;
      tableName = tablePair.second;
    } catch (IOException ioe) {
      throw new InternalException("Table name is wrong.", ioe);
    }

    // get table
    try {
      try {
        client = HCatalogUtil.getHiveMetaClient(catalogUri, null);
        table = HCatUtil.getTable(client, dbName, tableName);
        if (table != null) {
          exist = true;
        }
      } catch (NoSuchObjectException nsoe) {
        exist = false;
      } catch (Exception e) {
        throw new IOException(e);
      }
    } finally {
      HCatUtil.closeHiveClientQuietly(client);
    }

    return exist;
  }

  @Override
  public final TableDesc getTable(final String name) throws IOException {
    String dbName = null, tableName = null;
    Pair<String, String> tablePair = null;
    org.apache.hadoop.hive.ql.metadata.Table table = null;
    HiveMetaStoreClient client = null;
    Path path = null;
    CatalogProtos.StoreType storeType = null;
    org.apache.tajo.catalog.Schema schema = null;
    Options options = null;
    TableStats stats = null;
    PartitionDesc partitions = null;

    // get db name and table name.
    try {
      tablePair = HCatUtil.getDbAndTableName(name);
      dbName = tablePair.first;
      tableName = tablePair.second;
    } catch (IOException ioe) {
      throw new InternalException("Table name is wrong.", ioe);
    }

    //////////////////////////////////
    // set tajo table schema.
    //////////////////////////////////
    try {
      // get hive table schema
      try {
        client = HCatalogUtil.getHiveMetaClient(catalogUri, null);
        table = HCatUtil.getTable(client, dbName, tableName);
        path = table.getPath();
      } catch (NoSuchObjectException nsoe) {
        throw new InternalException("Table not found. - tableName:" + name, nsoe);
      } catch (Exception e) {
        throw new IOException(e);
      }

      // convert hcatalog field schema into tajo field schema.
      schema = new org.apache.tajo.catalog.Schema();
      HCatSchema tableSchema = HCatUtil.getTableSchemaWithPtnCols(table);
      List<HCatFieldSchema> fieldSchemaList = tableSchema.getFields();
      for (HCatFieldSchema eachField : fieldSchemaList) {
        String fieldName = tableName + "." + eachField.getName();
        TajoDataTypes.Type dataType = HCatalogUtil.getTajoFieldType(eachField.getType().toString());
        schema.addColumn(fieldName, dataType);
      }

      // validate field schema.
      try {
        HCatalogUtil.validateHCatTableAndTajoSchema(tableSchema);
      } catch (IOException e) {
        throw new InternalException(
            "HCatalog cannot support schema. - schema:" + tableSchema.toString(), e);
      }

      stats = new TableStats();
      options = Options.create();
      Properties properties = table.getMetadata();
      if (properties != null) {
        // set field delimiter
        String fieldDelimiter = "", fileOutputformat = "";
        if (properties.getProperty("field.delim") != null) {
          fieldDelimiter = properties.getProperty("field.delim");
        }
        // set file output format
        fileOutputformat = properties.getProperty("file.outputformat");
        storeType = CatalogUtil.getStoreType(HCatalogUtil.getStoreType(fileOutputformat,
            fieldDelimiter));

        // TODO: another stored file
        if (storeType.equals(CatalogProtos.StoreType.CSV) && fieldDelimiter != null) {
          options.put("csvfile.delimiter", fieldDelimiter);
        }

        // set data size
        if(properties.getProperty("totalSize") != null) {
          stats.setNumBytes(new Long(properties.getProperty("totalSize")));
        }
      }

      // set partition keys
      if (table.getPartitionKeys() != null) {
        if (table.getPartitionKeys().size() > 0) {
          partitions = new PartitionDesc();
          List<FieldSchema> partitionKeys = table.getPartitionKeys();
          for(int i = 0; i < partitionKeys.size(); i++) {
            FieldSchema fieldSchema = partitionKeys.get(i);
            TajoDataTypes.Type dataType = HCatalogUtil.getTajoFieldType(fieldSchema.getType().toString());
            partitions.addColumn(new Column(fieldSchema.getName(), dataType));
          }
          partitions.setPartitionsType(CatalogProtos.PartitionsType.COLUMN);
        }
      }
    } finally {
      HCatUtil.closeHiveClientQuietly(client);
    }
    TableMeta meta = new TableMeta(storeType, options);

    TableDesc tableDesc = new TableDesc(tableName, schema, meta, path);
    if (stats != null) {
      tableDesc.setStats(stats);
    }
    if (partitions != null) {
      tableDesc.setPartitions(partitions);
    }
    return tableDesc;
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
  public final List<String> getAllTableNames() throws IOException {
    List<String> dbs = null;
    List<String> tables = null;
    List<String> allTables = new ArrayList<String>();
    HiveMetaStoreClient client = null;

    try {
      try {
        client = HCatalogUtil.getHiveMetaClient(catalogUri, null);
        dbs = client.getAllDatabases();
        for(String eachDB: dbs) {
          tables = client.getAllTables(eachDB);
          for(String eachTable: tables) {
            allTables.add(eachDB + "." + eachTable);
          }
        }
      } catch (Exception e) {
        throw new IOException(e);
      }

    } finally {
      HCatUtil.closeHiveClientQuietly(client);
    }
    return allTables;
  }

  @Override
  public final void addTable(final TableDesc tableDesc) throws IOException {
    String dbName = null, tableName = null;
    Pair<String, String> tablePair = null;
    HiveMetaStoreClient client = null;

    // get db name and table name.
    try {
      tablePair = HCatUtil.getDbAndTableName(tableDesc.getName());
      dbName = tablePair.first;
      tableName = tablePair.second;
    } catch (IOException ioe) {
      throw new InternalException("Table name is wrong.", ioe);
    }

    try {
      try {
        client = HCatalogUtil.getHiveMetaClient(catalogUri, null);

        org.apache.hadoop.hive.metastore.api.Table table = new org.apache.hadoop.hive.metastore.api
            .Table();

        table.setDbName(dbName);
        table.setTableName(tableName);
        // TODO: set owner
        //table.setOwner();

        StorageDescriptor sd = new StorageDescriptor();

        // if tajo set location method, thrift client make exception as follows:
        // Caused by: MetaException(message:java.lang.NullPointerException)
        // If you want to modify table path, you have to modify on Hive cli.
        //sd.setLocation(tableDesc.getPath().toString());

        // set column information
        ArrayList<FieldSchema> cols = new ArrayList<FieldSchema>(tableDesc.getSchema().getColumns
            ().size());
        for (Column col : tableDesc.getSchema().getColumns()) {
          cols.add(new FieldSchema(col.getColumnName(), HCatalogUtil.getHiveFieldType(col
              .getDataType
                  ().getType().name()), ""));
        }
        sd.setCols(cols);

        // TODO: compression tyoe
        // n table type
        sd.setCompressed(false);

        sd.setParameters(new HashMap<String, String>());
        sd.setSerdeInfo(new SerDeInfo());
        sd.getSerdeInfo().setName(table.getTableName());

        // TODO: another Serialization librarys
        sd.getSerdeInfo().setSerializationLib(
            org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe.class.getName());

        sd.getSerdeInfo().setParameters(new HashMap<String, String>());
//      sd.getSerdeInfo().getParameters().put(serdeConstants.SERIALIZATION_FORMAT, "1");
        sd.getSerdeInfo().getParameters().put(serdeConstants.FIELD_DELIM, "|");

        // TODO: another input format classes
        sd.setInputFormat(org.apache.hadoop.mapred.TextInputFormat.class.getName());

        // TODO: another output format classes
        sd.setOutputFormat(org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat.class.getName
            ());

        sd.setSortCols(new ArrayList<Order>());

        table.setSd(sd);
        client.createTable(table);
      } catch (Exception e) {
        throw new IOException(e);
      }
    } finally {
      HCatUtil.closeHiveClientQuietly(client);
    }
  }

  @Override
  public final void deleteTable(final String name) throws IOException {
    String dbName = null, tableName = null;
    Pair<String, String> tablePair = null;
    HiveMetaStoreClient client = null;

    // get db name and table name.
    try {
      tablePair = HCatUtil.getDbAndTableName(name);
      dbName = tablePair.first;
      tableName = tablePair.second;
    } catch (IOException ioe) {
      throw new InternalException("Table name is wrong.", ioe);
    }

    try {
      client = HCatalogUtil.getHiveMetaClient(catalogUri, null);
      client.dropTable(dbName, tableName);
    } catch (NoSuchObjectException nsoe) {
    } catch (Exception e) {
      throw new IOException(e);
    } finally {
      HCatUtil.closeHiveClientQuietly(client);
    }
  }
  @Override
  public final void addFunction(final FunctionDesc func) throws IOException {
    // TODO - not implemented yet
  }

  @Override
  public final void deleteFunction(final FunctionDesc func) throws IOException {
    // TODO - not implemented yet
  }

  @Override
  public final void existFunction(final FunctionDesc func) throws IOException {
    // TODO - not implemented yet
  }

  @Override
  public final List<String> getAllFunctionNames() throws IOException {
    // TODO - not implemented yet
    return null;
  }

  @Override
  public void delIndex(String indexName) throws IOException {
    // TODO - not implemented yet
  }

  @Override
  public boolean existIndex(String indexName) throws IOException {
    // TODO - not implemented yet
    return false;
  }

  @Override
  public CatalogProtos.IndexDescProto[] getIndexes(String tableName) throws IOException {
    // TODO - not implemented yet
    return null;
  }

  @Override
  public void addIndex(CatalogProtos.IndexDescProto proto) throws IOException {
    // TODO - not implemented yet
  }

  @Override
  public CatalogProtos.IndexDescProto getIndex(String indexName) throws IOException {
    // TODO - not implemented yet
    return null;
  }

  @Override
  public CatalogProtos.IndexDescProto getIndex(String tableName, String columnName)
      throws IOException {
    // TODO - not implemented yet
    return null;
  }

  @Override
  public boolean existIndex(String tableName, String columnName) {
    // TODO - not implemented yet
    return false;
  }

  @Override
  public final void close() {
  }
}
