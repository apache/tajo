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

import com.google.common.collect.Lists;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe;
import org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinarySerDe;
import org.apache.hcatalog.common.HCatUtil;
import org.apache.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hcatalog.data.schema.HCatSchema;
import org.apache.tajo.TajoConstants;
import org.apache.tajo.catalog.*;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.exception.*;
import org.apache.tajo.catalog.partition.PartitionMethodDesc;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.common.exception.NotImplementedException;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.exception.InternalException;
import org.apache.tajo.storage.StorageConstants;
import org.apache.tajo.util.KeyValueSet;

import java.io.IOException;
import java.util.*;

import static org.apache.tajo.catalog.proto.CatalogProtos.PartitionType;
import static org.apache.tajo.catalog.proto.CatalogProtos.TablespaceProto;

public class HCatalogStore extends CatalogConstants implements CatalogStore {
  protected final Log LOG = LogFactory.getLog(getClass());

  private static String HIVE_WAREHOUSE_DIR_CONF_KEY = "hive.metastore.warehouse.dir";

  protected Configuration conf;
  private static final int CLIENT_POOL_SIZE = 2;
  private final HCatalogStoreClientPool clientPool;
  private final String defaultTableSpaceUri;

  public HCatalogStore(final Configuration conf) throws InternalException {
    if (!(conf instanceof TajoConf)) {
      throw new CatalogException("Invalid Configuration Type:" + conf.getClass().getSimpleName());
    }
    this.conf = conf;
    this.defaultTableSpaceUri = TajoConf.getWarehouseDir((TajoConf) conf).toString();
    this.clientPool = new HCatalogStoreClientPool(CLIENT_POOL_SIZE, conf);
  }

  @Override
  public boolean existTable(final String databaseName, final String tableName) throws CatalogException {
    boolean exist = false;
    org.apache.hadoop.hive.ql.metadata.Table table = null;
    HCatalogStoreClientPool.HCatalogStoreClient client = null;

    // get table
    try {
      client = clientPool.getClient();
      table = HCatUtil.getTable(client.getHiveClient(), databaseName, tableName);
      if (table != null) {
        exist = true;
      }
    } catch (NoSuchObjectException nsoe) {
      exist = false;
    } catch (Exception e) {
      throw new CatalogException(e);
    } finally {
      if (client != null) {
        client.release();
      }
    }

    return exist;
  }

  @Override
  public final CatalogProtos.TableDescProto getTable(String databaseName, final String tableName) throws CatalogException {
    org.apache.hadoop.hive.ql.metadata.Table table = null;
    HCatalogStoreClientPool.HCatalogStoreClient client = null;
    Path path = null;
    CatalogProtos.StoreType storeType = null;
    org.apache.tajo.catalog.Schema schema = null;
    KeyValueSet options = null;
    TableStats stats = null;
    PartitionMethodDesc partitions = null;

    //////////////////////////////////
    // set tajo table schema.
    //////////////////////////////////
    try {
      // get hive table schema
      try {
        client = clientPool.getClient();
        table = HCatUtil.getTable(client.getHiveClient(), databaseName, tableName);
        path = table.getPath();
      } catch (NoSuchObjectException nsoe) {
        throw new CatalogException("Table not found. - tableName:" + tableName, nsoe);
      } catch (Exception e) {
        throw new CatalogException(e);
      }

      // convert hcatalog field schema into tajo field schema.
      schema = new org.apache.tajo.catalog.Schema();
      HCatSchema tableSchema = null;

      try {
        tableSchema = HCatUtil.getTableSchemaWithPtnCols(table);
      } catch (IOException ioe) {
        throw new CatalogException("Fail to get table schema. - tableName:" + tableName, ioe);
      }
      List<HCatFieldSchema> fieldSchemaList = tableSchema.getFields();
      boolean isPartitionKey = false;
      for (HCatFieldSchema eachField : fieldSchemaList) {
        isPartitionKey = false;

        if (table.getPartitionKeys() != null) {
          for (FieldSchema partitionKey : table.getPartitionKeys()) {
            if (partitionKey.getName().equals(eachField.getName())) {
              isPartitionKey = true;
            }
          }
        }

        if (!isPartitionKey) {
          String fieldName = databaseName + CatalogConstants.IDENTIFIER_DELIMITER + tableName +
              CatalogConstants.IDENTIFIER_DELIMITER + eachField.getName();
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
      options = new KeyValueSet();
      options.putAll(table.getParameters());
      options.delete("EXTERNAL");

      Properties properties = table.getMetadata();
      if (properties != null) {
        // set field delimiter
        String fieldDelimiter = "", nullFormat = "";
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
        options.delete(serdeConstants.SERIALIZATION_NULL_FORMAT);

        // set file output format
        String fileOutputformat = properties.getProperty(hive_metastoreConstants.FILE_OUTPUT_FORMAT);
        storeType = CatalogUtil.getStoreType(HCatalogUtil.getStoreType(fileOutputformat));

        if (storeType.equals(CatalogProtos.StoreType.CSV)) {
          options.put(StorageConstants.CSVFILE_DELIMITER, StringEscapeUtils.escapeJava(fieldDelimiter));
          options.put(StorageConstants.CSVFILE_NULL, StringEscapeUtils.escapeJava(nullFormat));
        } else if (storeType.equals(CatalogProtos.StoreType.RCFILE)) {
          options.put(StorageConstants.RCFILE_NULL, StringEscapeUtils.escapeJava(nullFormat));
          String serde = properties.getProperty(serdeConstants.SERIALIZATION_LIB);
          if (LazyBinaryColumnarSerDe.class.getName().equals(serde)) {
            options.put(StorageConstants.RCFILE_SERDE, StorageConstants.DEFAULT_BINARY_SERDE);
          } else if (ColumnarSerDe.class.getName().equals(serde)) {
            options.put(StorageConstants.RCFILE_SERDE, StorageConstants.DEFAULT_TEXT_SERDE);
          }
        } else if (storeType.equals(CatalogProtos.StoreType.SEQUENCEFILE) ) {
          options.put(StorageConstants.SEQUENCEFILE_DELIMITER, StringEscapeUtils.escapeJava(fieldDelimiter));
          options.put(StorageConstants.SEQUENCEFILE_NULL, StringEscapeUtils.escapeJava(nullFormat));
          String serde = properties.getProperty(serdeConstants.SERIALIZATION_LIB);
          if (LazyBinarySerDe.class.getName().equals(serde)) {
            options.put(StorageConstants.SEQUENCEFILE_SERDE, StorageConstants.DEFAULT_BINARY_SERDE);
          } else if (LazySimpleSerDe.class.getName().equals(serde)) {
            options.put(StorageConstants.SEQUENCEFILE_SERDE, StorageConstants.DEFAULT_TEXT_SERDE);
          }
        }

        // set data size
        long totalSize = 0;
        if (properties.getProperty("totalSize") != null) {
          totalSize = Long.parseLong(properties.getProperty("totalSize"));
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
      List<FieldSchema> partitionKeys = table.getPartitionKeys();

      if (null != partitionKeys) {
        Schema expressionSchema = new Schema();
        StringBuilder sb = new StringBuilder();
        if (partitionKeys.size() > 0) {
          for (int i = 0; i < partitionKeys.size(); i++) {
            FieldSchema fieldSchema = partitionKeys.get(i);
            TajoDataTypes.Type dataType = HCatalogUtil.getTajoFieldType(fieldSchema.getType().toString());
            String fieldName = databaseName + CatalogConstants.IDENTIFIER_DELIMITER + tableName +
                CatalogConstants.IDENTIFIER_DELIMITER + fieldSchema.getName();
            expressionSchema.addColumn(new Column(fieldName, dataType));
            if (i > 0) {
              sb.append(",");
            }
            sb.append(fieldSchema.getName());
          }
          partitions = new PartitionMethodDesc(
              databaseName,
              tableName,
              PartitionType.COLUMN,
              sb.toString(),
              expressionSchema);
        }
      }
    } finally {
      if(client != null) client.release();
    }
    TableMeta meta = new TableMeta(storeType, options);
    TableDesc tableDesc = new TableDesc(databaseName + "." + tableName, schema, meta, path);
    if (table.getTableType().equals(TableType.EXTERNAL_TABLE)) {
      tableDesc.setExternal(true);
    }
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
  public final List<String> getAllTableNames(String databaseName) throws CatalogException {
    HCatalogStoreClientPool.HCatalogStoreClient client = null;

    try {
      client = clientPool.getClient();
      return client.getHiveClient().getAllTables(databaseName);
    } catch (MetaException e) {
      throw new CatalogException(e);
    } finally {
      if(client != null) client.release();
    }
  }

  @Override
  public void createTablespace(String spaceName, String spaceUri) throws CatalogException {
    // SKIP
  }

  @Override
  public boolean existTablespace(String spaceName) throws CatalogException {
    // SKIP
    return spaceName.equals(TajoConstants.DEFAULT_TABLESPACE_NAME);
  }

  @Override
  public void dropTablespace(String spaceName) throws CatalogException {
    // SKIP
  }

  @Override
  public Collection<String> getAllTablespaceNames() throws CatalogException {
    return Lists.newArrayList(TajoConstants.DEFAULT_TABLESPACE_NAME);
  }

  @Override
  public TablespaceProto getTablespace(String spaceName) throws CatalogException {
    if (spaceName.equals(TajoConstants.DEFAULT_TABLESPACE_NAME)) {
      TablespaceProto.Builder builder = TablespaceProto.newBuilder();
      builder.setSpaceName(TajoConstants.DEFAULT_TABLESPACE_NAME);
      builder.setUri(defaultTableSpaceUri);
      return builder.build();
    } else {
      throw new CatalogException("tablespace concept is not supported in HCatalogStore");
    }
  }

  @Override
  public void alterTablespace(CatalogProtos.AlterTablespaceProto alterProto) throws CatalogException {
    throw new CatalogException("tablespace concept is not supported in HCatalogStore");
  }

  @Override
  public void createDatabase(String databaseName, String tablespaceName) throws CatalogException {
    HCatalogStoreClientPool.HCatalogStoreClient client = null;

    try {
      Database database = new Database(
          databaseName,
          "",
          defaultTableSpaceUri + "/" + databaseName,
          new HashMap<String, String>());
      client = clientPool.getClient();
      client.getHiveClient().createDatabase(database);
    } catch (AlreadyExistsException e) {
      throw new AlreadyExistsDatabaseException(databaseName);
    } catch (Throwable t) {
      throw new CatalogException(t);
    } finally {
      if (client != null) {
        client.release();
      }
    }
  }

  @Override
  public boolean existDatabase(String databaseName) throws CatalogException {
    HCatalogStoreClientPool.HCatalogStoreClient client = null;

    try {
      client = clientPool.getClient();
      List<String> databaseNames = client.getHiveClient().getAllDatabases();
      return databaseNames.contains(databaseName);
    } catch (Throwable t) {
      throw new CatalogException(t);
    } finally {
      if (client != null) {
        client.release();
      }
    }
  }

  @Override
  public void dropDatabase(String databaseName) throws CatalogException {
    HCatalogStoreClientPool.HCatalogStoreClient client = null;

    try {
      client = clientPool.getClient();
      client.getHiveClient().dropDatabase(databaseName);
    } catch (NoSuchObjectException e) {
      throw new NoSuchDatabaseException(databaseName);
    } catch (Throwable t) {
      throw new CatalogException(databaseName);
    } finally {
      if (client != null) {
        client.release();
      }
    }
  }

  @Override
  public Collection<String> getAllDatabaseNames() throws CatalogException {
    HCatalogStoreClientPool.HCatalogStoreClient client = null;

    try {
      client = clientPool.getClient();
      return client.getHiveClient().getAllDatabases();
    } catch (MetaException e) {
      throw new CatalogException(e);
    } finally {
      if (client != null) {
        client.release();
      }
    }
  }

  @Override
  public final void createTable(final CatalogProtos.TableDescProto tableDescProto) throws CatalogException {
    HCatalogStoreClientPool.HCatalogStoreClient client = null;

    TableDesc tableDesc = new TableDesc(tableDescProto);
    String[] splitted = CatalogUtil.splitFQTableName(tableDesc.getName());
    String databaseName = splitted[0];
    String tableName = splitted[1];

    try {
      client = clientPool.getClient();

      org.apache.hadoop.hive.metastore.api.Table table = new org.apache.hadoop.hive.metastore.api.Table();
      table.setDbName(databaseName);
      table.setTableName(tableName);
      table.setParameters(new HashMap<String, String>(tableDesc.getMeta().getOptions().getAllKeyValus()));
      // TODO: set owner
      //table.setOwner();

      StorageDescriptor sd = new StorageDescriptor();
      sd.setSerdeInfo(new SerDeInfo());
      sd.getSerdeInfo().setParameters(new HashMap<String, String>());
      sd.getSerdeInfo().setName(table.getTableName());

      // if tajo set location method, thrift client make exception as follows:
      // Caused by: MetaException(message:java.lang.NullPointerException)
      // If you want to modify table path, you have to modify on Hive cli.
      if (tableDesc.isExternal()) {
        table.setTableType(TableType.EXTERNAL_TABLE.name());
        table.putToParameters("EXTERNAL", "TRUE");

        FileSystem fs = tableDesc.getPath().getFileSystem(conf);
        if (fs.isFile(tableDesc.getPath())) {
          LOG.warn("A table path is a file, but HCatalog does not allow a file path.");
          sd.setLocation(tableDesc.getPath().getParent().toString());
        } else {
          sd.setLocation(tableDesc.getPath().toString());
        }
      }

      // set column information
      List<Column> columns = tableDesc.getSchema().getColumns();
      ArrayList<FieldSchema> cols = new ArrayList<FieldSchema>(columns.size());

      for (Column eachField : columns) {
        cols.add(new FieldSchema(eachField.getSimpleName(),
            HCatalogUtil.getHiveFieldType(eachField.getDataType()), ""));
      }
      sd.setCols(cols);

      // set partition keys
      if (tableDesc.hasPartition() && tableDesc.getPartitionMethod().getPartitionType().equals(PartitionType.COLUMN)) {
        List<FieldSchema> partitionKeys = new ArrayList<FieldSchema>();
        for (Column eachPartitionKey : tableDesc.getPartitionMethod().getExpressionSchema().getColumns()) {
          partitionKeys.add(new FieldSchema(eachPartitionKey.getSimpleName(),
              HCatalogUtil.getHiveFieldType(eachPartitionKey.getDataType()), ""));
        }
        table.setPartitionKeys(partitionKeys);
      }

      if (tableDesc.getMeta().getStoreType().equals(CatalogProtos.StoreType.RCFILE)) {
        String serde = tableDesc.getMeta().getOption(StorageConstants.RCFILE_SERDE);
        sd.setInputFormat(org.apache.hadoop.hive.ql.io.RCFileInputFormat.class.getName());
        sd.setOutputFormat(org.apache.hadoop.hive.ql.io.RCFileOutputFormat.class.getName());
        if (StorageConstants.DEFAULT_TEXT_SERDE.equals(serde)) {
          sd.getSerdeInfo().setSerializationLib(org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe.class.getName());
        } else {
          sd.getSerdeInfo().setSerializationLib(
              org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe.class.getName());
        }

        if (tableDesc.getMeta().getOption(StorageConstants.RCFILE_NULL) != null) {
          table.putToParameters(serdeConstants.SERIALIZATION_NULL_FORMAT,
              StringEscapeUtils.unescapeJava(tableDesc.getMeta().getOption(StorageConstants.RCFILE_NULL)));
        }
      } else if (tableDesc.getMeta().getStoreType().equals(CatalogProtos.StoreType.CSV)) {
        sd.getSerdeInfo().setSerializationLib(org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe.class.getName());
        sd.setInputFormat(org.apache.hadoop.mapred.TextInputFormat.class.getName());
        sd.setOutputFormat(org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat.class.getName());

        String fieldDelimiter = tableDesc.getMeta().getOption(StorageConstants.CSVFILE_DELIMITER,
            StorageConstants.DEFAULT_FIELD_DELIMITER);

        // User can use an unicode for filed delimiter such as \u0001, \001.
        // In this case, java console will convert this value into "\\u001".
        // And hive will un-espace this value again.
        // As a result, user can use right field delimiter.
        // So, we have to un-escape this value.
        sd.getSerdeInfo().putToParameters(serdeConstants.SERIALIZATION_FORMAT,
            StringEscapeUtils.unescapeJava(fieldDelimiter));
        sd.getSerdeInfo().putToParameters(serdeConstants.FIELD_DELIM,
            StringEscapeUtils.unescapeJava(fieldDelimiter));
        table.getParameters().remove(StorageConstants.CSVFILE_DELIMITER);

        if (tableDesc.getMeta().getOption(StorageConstants.CSVFILE_NULL) != null) {
          table.putToParameters(serdeConstants.SERIALIZATION_NULL_FORMAT,
              StringEscapeUtils.unescapeJava(tableDesc.getMeta().getOption(StorageConstants.CSVFILE_NULL)));
          table.getParameters().remove(StorageConstants.CSVFILE_NULL);
        }
      } else if (tableDesc.getMeta().getStoreType().equals(CatalogProtos.StoreType.SEQUENCEFILE)) {
        String serde = tableDesc.getMeta().getOption(StorageConstants.SEQUENCEFILE_SERDE);
        sd.setInputFormat(org.apache.hadoop.mapred.SequenceFileInputFormat.class.getName());
        sd.setOutputFormat(org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat.class.getName());

        if (StorageConstants.DEFAULT_TEXT_SERDE.equals(serde)) {
          sd.getSerdeInfo().setSerializationLib(org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe.class.getName());

          String fieldDelimiter = tableDesc.getMeta().getOption(StorageConstants.SEQUENCEFILE_DELIMITER,
              StorageConstants.DEFAULT_FIELD_DELIMITER);

          // User can use an unicode for filed delimiter such as \u0001, \001.
          // In this case, java console will convert this value into "\\u001".
          // And hive will un-espace this value again.
          // As a result, user can use right field delimiter.
          // So, we have to un-escape this value.
          sd.getSerdeInfo().putToParameters(serdeConstants.SERIALIZATION_FORMAT,
              StringEscapeUtils.unescapeJava(fieldDelimiter));
          sd.getSerdeInfo().putToParameters(serdeConstants.FIELD_DELIM,
              StringEscapeUtils.unescapeJava(fieldDelimiter));
          table.getParameters().remove(StorageConstants.SEQUENCEFILE_DELIMITER);
        } else {
          sd.getSerdeInfo().setSerializationLib(org.apache.hadoop.hive.serde2.lazybinary.LazyBinarySerDe.class.getName());
        }

        if (tableDesc.getMeta().getOption(StorageConstants.SEQUENCEFILE_NULL) != null) {
          table.putToParameters(serdeConstants.SERIALIZATION_NULL_FORMAT,
              StringEscapeUtils.unescapeJava(tableDesc.getMeta().getOption(StorageConstants.SEQUENCEFILE_NULL)));
          table.getParameters().remove(StorageConstants.SEQUENCEFILE_NULL);
        }
      } else {
        if (tableDesc.getMeta().getStoreType().equals(CatalogProtos.StoreType.PARQUET)) {
          sd.setInputFormat(parquet.hive.DeprecatedParquetInputFormat.class.getName());
          sd.setOutputFormat(parquet.hive.DeprecatedParquetOutputFormat.class.getName());
          sd.getSerdeInfo().setSerializationLib(parquet.hive.serde.ParquetHiveSerDe.class.getName());
        } else {
          throw new CatalogException(new NotImplementedException(tableDesc.getMeta().getStoreType
              ().name()));
        }
      }

      sd.setSortCols(new ArrayList<Order>());

      table.setSd(sd);
      client.getHiveClient().createTable(table);
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new CatalogException(e);
    } finally {
      if(client != null) client.release();
    }
  }

  @Override
  public final void dropTable(String databaseName, final String tableName) throws CatalogException {
    HCatalogStoreClientPool.HCatalogStoreClient client = null;

    try {
      client = clientPool.getClient();
      client.getHiveClient().dropTable(databaseName, tableName, false, false);
    } catch (NoSuchObjectException nsoe) {
    } catch (Exception e) {
      throw new CatalogException(e);
    } finally {
      if (client != null) {
        client.release();
      }
    }
  }


  @Override
  public void alterTable(final CatalogProtos.AlterTableDescProto alterTableDescProto) throws CatalogException {
    final String[] split = CatalogUtil.splitFQTableName(alterTableDescProto.getTableName());

    if (split.length == 1) {
      throw new IllegalArgumentException("alterTable() requires a qualified table name, but it is \""
          + alterTableDescProto.getTableName() + "\".");
    }

    final String databaseName = split[0];
    final String tableName = split[1];


    switch (alterTableDescProto.getAlterTableType()) {
      case RENAME_TABLE:
        if (existTable(databaseName,alterTableDescProto.getNewTableName().toLowerCase())) {
          throw new AlreadyExistsTableException(alterTableDescProto.getNewTableName());
        }
        renameTable(databaseName, tableName, alterTableDescProto.getNewTableName().toLowerCase());
        break;
      case RENAME_COLUMN:
        if (existColumn(databaseName,tableName, alterTableDescProto.getAlterColumnName().getNewColumnName())) {
          throw new ColumnNameAlreadyExistException(alterTableDescProto.getAlterColumnName().getNewColumnName());
        }
        renameColumn(databaseName, tableName, alterTableDescProto.getAlterColumnName());
        break;
      case ADD_COLUMN:
        if (existColumn(databaseName,tableName, alterTableDescProto.getAddColumn().getName())) {
          throw new ColumnNameAlreadyExistException(alterTableDescProto.getAddColumn().getName());
        }
        addNewColumn(databaseName, tableName, alterTableDescProto.getAddColumn());
        break;
      default:
        //TODO
    }
  }


  private void renameTable(String databaseName, String tableName, String newTableName) {
    HCatalogStoreClientPool.HCatalogStoreClient client = null;
    try {
      client = clientPool.getClient();
      Table newTable = client.getHiveClient().getTable(databaseName, tableName);
      newTable.setTableName(newTableName);
      client.getHiveClient().alter_table(databaseName, tableName, newTable);

    } catch (NoSuchObjectException nsoe) {
    } catch (Exception e) {
      throw new CatalogException(e);
    } finally {
      if (client != null) {
        client.release();
      }
    }
  }

  private void renameColumn(String databaseName, String tableName, CatalogProtos.AlterColumnProto alterColumnProto) {
    HCatalogStoreClientPool.HCatalogStoreClient client = null;
    try {

      client = clientPool.getClient();
      Table table = client.getHiveClient().getTable(databaseName, tableName);
      List<FieldSchema> columns = table.getSd().getCols();

      for (final FieldSchema currentColumn : columns) {
        if (currentColumn.getName().equalsIgnoreCase(alterColumnProto.getOldColumnName())) {
          currentColumn.setName(alterColumnProto.getNewColumnName());
        }
      }
      client.getHiveClient().alter_table(databaseName, tableName, table);

    } catch (NoSuchObjectException nsoe) {
    } catch (Exception e) {
      throw new CatalogException(e);
    } finally {
      if (client != null) {
        client.release();
      }
    }
  }


  private void addNewColumn(String databaseName, String tableName, CatalogProtos.ColumnProto columnProto) {
    HCatalogStoreClientPool.HCatalogStoreClient client = null;
    try {

      client = clientPool.getClient();
      Table table = client.getHiveClient().getTable(databaseName, tableName);
      List<FieldSchema> columns = table.getSd().getCols();
      columns.add(new FieldSchema(columnProto.getName(),
          HCatalogUtil.getHiveFieldType(columnProto.getDataType()), ""));
      client.getHiveClient().alter_table(databaseName, tableName, table);


    } catch (NoSuchObjectException nsoe) {
    } catch (Exception e) {
      throw new CatalogException(e);
    } finally {
      if (client != null) {
        client.release();
      }
    }
  }

  @Override
  public void addPartitionMethod(CatalogProtos.PartitionMethodProto partitionMethodProto) throws CatalogException {
    // TODO - not implemented yet
  }

  @Override
  public CatalogProtos.PartitionMethodProto getPartitionMethod(String databaseName, String tableName)
      throws CatalogException {
    return null;  // TODO - not implemented yet
  }

  @Override
  public boolean existPartitionMethod(String databaseName, String tableName) throws CatalogException {
    return false;  // TODO - not implemented yet
  }

  @Override
  public void dropPartitionMethod(String databaseName, String tableName) throws CatalogException {
    // TODO - not implemented yet
  }

  @Override
  public void addPartitions(CatalogProtos.PartitionsProto partitionsProto) throws CatalogException {
    // TODO - not implemented yet
  }

  @Override
  public void addPartition(String databaseName, String tableName, CatalogProtos.PartitionDescProto partitionDescProto) throws CatalogException {

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
  public void dropPartitions(String tableName) throws CatalogException {

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
  public void dropIndex(String databaseName, String indexName) throws CatalogException {
    // TODO - not implemented yet
  }

  @Override
  public boolean existIndexByName(String databaseName, String indexName) throws CatalogException {
    // TODO - not implemented yet
    return false;
  }

  @Override
  public CatalogProtos.IndexDescProto[] getIndexes(String databaseName, String tableName) throws CatalogException {
    // TODO - not implemented yet
    return null;
  }

  @Override
  public void createIndex(CatalogProtos.IndexDescProto proto) throws CatalogException {
    // TODO - not implemented yet
  }

  @Override
  public CatalogProtos.IndexDescProto getIndexByName(String databaseName, String indexName) throws CatalogException {
    // TODO - not implemented yet
    return null;
  }

  @Override
  public CatalogProtos.IndexDescProto getIndexByColumn(String databaseName, String tableName, String columnName)
      throws CatalogException {
    // TODO - not implemented yet
    return null;
  }

  @Override
  public boolean existIndexByColumn(String databaseName, String tableName, String columnName) throws CatalogException {
    // TODO - not implemented yet
    return false;
  }

  @Override
  public final void close() {
    clientPool.close();
  }

  private boolean existColumn(final String databaseName ,final String tableName , final String columnName) throws CatalogException {
    boolean exist = false;
    HCatalogStoreClientPool.HCatalogStoreClient client = null;

    try {

      client = clientPool.getClient();
      Table table = client.getHiveClient().getTable(databaseName, tableName);
      List<FieldSchema> columns = table.getSd().getCols();

      for (final FieldSchema currentColumn : columns) {
        if (currentColumn.getName().equalsIgnoreCase(columnName)) {
          exist = true;
        }
      }
      client.getHiveClient().alter_table(databaseName, tableName, table);

    } catch (NoSuchObjectException nsoe) {
    } catch (Exception e) {
      throw new CatalogException(e);
    } finally {
      if (client != null) {
        client.release();
      }
    }

    return exist;
  }
}
