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
import org.apache.tajo.BuiltinStorages;
import org.apache.tajo.TajoConstants;
import org.apache.tajo.catalog.*;
import org.apache.tajo.catalog.exception.*;
import org.apache.tajo.catalog.partition.PartitionMethodDesc;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.catalog.proto.CatalogProtos.ColumnProto;
import org.apache.tajo.catalog.proto.CatalogProtos.DatabaseProto;
import org.apache.tajo.catalog.proto.CatalogProtos.IndexProto;
import org.apache.tajo.catalog.proto.CatalogProtos.TableDescriptorProto;
import org.apache.tajo.catalog.proto.CatalogProtos.TableOptionProto;
import org.apache.tajo.catalog.proto.CatalogProtos.TablePartitionProto;
import org.apache.tajo.catalog.proto.CatalogProtos.TableStatsProto;
import org.apache.tajo.catalog.proto.CatalogProtos.TablespaceProto;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.common.exception.NotImplementedException;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.exception.InternalException;
import org.apache.tajo.storage.StorageConstants;
import org.apache.tajo.util.KeyValueSet;
import org.apache.thrift.TException;

import java.io.IOException;
import java.util.*;

import static org.apache.tajo.catalog.proto.CatalogProtos.PartitionType;

public class HiveCatalogStore extends CatalogConstants implements CatalogStore {
  protected final Log LOG = LogFactory.getLog(getClass());

  private static String HIVE_WAREHOUSE_DIR_CONF_KEY = "hive.metastore.warehouse.dir";

  protected Configuration conf;
  private static final int CLIENT_POOL_SIZE = 2;
  private final HiveCatalogStoreClientPool clientPool;
  private final String defaultTableSpaceUri;

  public HiveCatalogStore(final Configuration conf) throws InternalException {
    if (!(conf instanceof TajoConf)) {
      throw new CatalogException("Invalid Configuration Type:" + conf.getClass().getSimpleName());
    }
    this.conf = conf;
    this.defaultTableSpaceUri = TajoConf.getWarehouseDir((TajoConf) conf).toString();
    this.clientPool = new HiveCatalogStoreClientPool(CLIENT_POOL_SIZE, conf);
  }

  @Override
  public boolean existTable(final String databaseName, final String tableName) throws CatalogException {
    boolean exist = false;
    org.apache.hadoop.hive.ql.metadata.Table table;
    HiveCatalogStoreClientPool.HiveCatalogStoreClient client = null;

    // get table
    try {
      client = clientPool.getClient();
      table = HiveCatalogUtil.getTable(client.getHiveClient(), databaseName, tableName);
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
    HiveCatalogStoreClientPool.HiveCatalogStoreClient client = null;
    Path path = null;
    String storeType = null;
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
        table = HiveCatalogUtil.getTable(client.getHiveClient(), databaseName, tableName);
        path = table.getPath();
      } catch (NoSuchObjectException nsoe) {
        throw new CatalogException("Table not found. - tableName:" + tableName, nsoe);
      } catch (Exception e) {
        throw new CatalogException(e);
      }

      // convert HiveCatalogStore field schema into tajo field schema.
      schema = new org.apache.tajo.catalog.Schema();

      List<FieldSchema> fieldSchemaList = table.getCols();
      boolean isPartitionKey = false;
      for (FieldSchema eachField : fieldSchemaList) {
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
          TajoDataTypes.Type dataType = HiveCatalogUtil.getTajoFieldType(eachField.getType().toString());
          schema.addColumn(fieldName, dataType);
        }
      }

      // validate field schema.
      HiveCatalogUtil.validateSchema(table);

      stats = new TableStats();
      options = new KeyValueSet();
      options.putAll(table.getParameters());
      options.remove("EXTERNAL");

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
        options.remove(serdeConstants.SERIALIZATION_NULL_FORMAT);

        // set file output format
        String fileOutputformat = properties.getProperty(hive_metastoreConstants.FILE_OUTPUT_FORMAT);
        storeType = HiveCatalogUtil.getStoreType(fileOutputformat);

        if (storeType.equalsIgnoreCase("TEXT")) {
          options.set(StorageConstants.TEXT_DELIMITER, StringEscapeUtils.escapeJava(fieldDelimiter));
          options.set(StorageConstants.TEXT_NULL, StringEscapeUtils.escapeJava(nullFormat));
        } else if (storeType.equals("RCFILE")) {
          options.set(StorageConstants.RCFILE_NULL, StringEscapeUtils.escapeJava(nullFormat));
          String serde = properties.getProperty(serdeConstants.SERIALIZATION_LIB);
          if (LazyBinaryColumnarSerDe.class.getName().equals(serde)) {
            options.set(StorageConstants.RCFILE_SERDE, StorageConstants.DEFAULT_BINARY_SERDE);
          } else if (ColumnarSerDe.class.getName().equals(serde)) {
            options.set(StorageConstants.RCFILE_SERDE, StorageConstants.DEFAULT_TEXT_SERDE);
          }
        } else if (storeType.equals("SEQUENCEFILE") ) {
          options.set(StorageConstants.SEQUENCEFILE_DELIMITER, StringEscapeUtils.escapeJava(fieldDelimiter));
          options.set(StorageConstants.SEQUENCEFILE_NULL, StringEscapeUtils.escapeJava(nullFormat));
          String serde = properties.getProperty(serdeConstants.SERIALIZATION_LIB);
          if (LazyBinarySerDe.class.getName().equals(serde)) {
            options.set(StorageConstants.SEQUENCEFILE_SERDE, StorageConstants.DEFAULT_BINARY_SERDE);
          } else if (LazySimpleSerDe.class.getName().equals(serde)) {
            options.set(StorageConstants.SEQUENCEFILE_SERDE, StorageConstants.DEFAULT_TEXT_SERDE);
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
        org.apache.tajo.catalog.Schema expressionSchema = new org.apache.tajo.catalog.Schema();
        StringBuilder sb = new StringBuilder();
        if (partitionKeys.size() > 0) {
          for (int i = 0; i < partitionKeys.size(); i++) {
            FieldSchema fieldSchema = partitionKeys.get(i);
            TajoDataTypes.Type dataType = HiveCatalogUtil.getTajoFieldType(fieldSchema.getType().toString());
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
    TableDesc tableDesc = new TableDesc(databaseName + "." + tableName, schema, meta, path.toUri());
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
      LOG.error("Cannot find a matched type against from '" + typeStr + "'");
      return null;
    }
  }

  @Override
  public final List<String> getAllTableNames(String databaseName) throws CatalogException {
    HiveCatalogStoreClientPool.HiveCatalogStoreClient client = null;

    try {
      client = clientPool.getClient();
      return client.getHiveClient().getAllTables(databaseName);
    } catch (TException e) {
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
      throw new CatalogException("tablespace concept is not supported in HiveCatalogStore");
    }
  }

  @Override
  public void updateTableStats(CatalogProtos.UpdateTableStatsProto statsProto) throws
    CatalogException {
    // TODO - not implemented yet
  }

  @Override
  public void alterTablespace(CatalogProtos.AlterTablespaceProto alterProto) throws CatalogException {
    throw new CatalogException("tablespace concept is not supported in HiveCatalogStore");
  }

  @Override
  public void createDatabase(String databaseName, String tablespaceName) throws CatalogException {
    HiveCatalogStoreClientPool.HiveCatalogStoreClient client = null;

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
    HiveCatalogStoreClientPool.HiveCatalogStoreClient client = null;

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
    HiveCatalogStoreClientPool.HiveCatalogStoreClient client = null;

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
    HiveCatalogStoreClientPool.HiveCatalogStoreClient client = null;

    try {
      client = clientPool.getClient();
      return client.getHiveClient().getAllDatabases();
    } catch (TException e) {
      throw new CatalogException(e);
    } finally {
      if (client != null) {
        client.release();
      }
    }
  }

  @Override
  public final void createTable(final CatalogProtos.TableDescProto tableDescProto) throws CatalogException {
    HiveCatalogStoreClientPool.HiveCatalogStoreClient client = null;

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

        Path tablePath = new Path(tableDesc.getPath());
        FileSystem fs = tablePath.getFileSystem(conf);
        if (fs.isFile(tablePath)) {
          LOG.warn("A table path is a file, but HiveCatalogStore does not allow a file path.");
          sd.setLocation(tablePath.getParent().toString());
        } else {
          sd.setLocation(tablePath.toString());
        }
      }

      // set column information
      List<Column> columns = tableDesc.getSchema().getRootColumns();
      ArrayList<FieldSchema> cols = new ArrayList<FieldSchema>(columns.size());

      for (Column eachField : columns) {
        cols.add(new FieldSchema(eachField.getSimpleName(),
            HiveCatalogUtil.getHiveFieldType(eachField.getDataType()), ""));
      }
      sd.setCols(cols);

      // set partition keys
      if (tableDesc.hasPartition() && tableDesc.getPartitionMethod().getPartitionType().equals(PartitionType.COLUMN)) {
        List<FieldSchema> partitionKeys = new ArrayList<FieldSchema>();
        for (Column eachPartitionKey : tableDesc.getPartitionMethod().getExpressionSchema().getRootColumns()) {
          partitionKeys.add(new FieldSchema(eachPartitionKey.getSimpleName(),
              HiveCatalogUtil.getHiveFieldType(eachPartitionKey.getDataType()), ""));
        }
        table.setPartitionKeys(partitionKeys);
      }

      if (tableDesc.getMeta().getStoreType().equalsIgnoreCase(BuiltinStorages.RCFILE)) {
        String serde = tableDesc.getMeta().getOption(StorageConstants.RCFILE_SERDE);
        sd.setInputFormat(org.apache.hadoop.hive.ql.io.RCFileInputFormat.class.getName());
        sd.setOutputFormat(org.apache.hadoop.hive.ql.io.RCFileOutputFormat.class.getName());
        if (StorageConstants.DEFAULT_TEXT_SERDE.equals(serde)) {
          sd.getSerdeInfo().setSerializationLib(org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe.class.getName());
        } else {
          sd.getSerdeInfo().setSerializationLib(
              org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe.class.getName());
        }

        if (tableDesc.getMeta().getOptions().containsKey(StorageConstants.RCFILE_NULL)) {
          table.putToParameters(serdeConstants.SERIALIZATION_NULL_FORMAT,
              StringEscapeUtils.unescapeJava(tableDesc.getMeta().getOption(StorageConstants.RCFILE_NULL)));
        }
      } else if (tableDesc.getMeta().getStoreType().equalsIgnoreCase(BuiltinStorages.CSV)
          || tableDesc.getMeta().getStoreType().equals(CatalogProtos.StoreType.TEXTFILE)) {
        sd.getSerdeInfo().setSerializationLib(org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe.class.getName());
        sd.setInputFormat(org.apache.hadoop.mapred.TextInputFormat.class.getName());
        sd.setOutputFormat(org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat.class.getName());

        String fieldDelimiter = tableDesc.getMeta().getOption(StorageConstants.TEXT_DELIMITER,
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
        table.getParameters().remove(StorageConstants.TEXT_DELIMITER);

        if (tableDesc.getMeta().containsOption(StorageConstants.TEXT_NULL)) {
          table.putToParameters(serdeConstants.SERIALIZATION_NULL_FORMAT,
              StringEscapeUtils.unescapeJava(tableDesc.getMeta().getOption(StorageConstants.TEXT_NULL)));
          table.getParameters().remove(StorageConstants.TEXT_NULL);
        }
      } else if (tableDesc.getMeta().getStoreType().equalsIgnoreCase(BuiltinStorages.SEQUENCE_FILE)) {
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

        if (tableDesc.getMeta().containsOption(StorageConstants.SEQUENCEFILE_NULL)) {
          table.putToParameters(serdeConstants.SERIALIZATION_NULL_FORMAT,
              StringEscapeUtils.unescapeJava(tableDesc.getMeta().getOption(StorageConstants.SEQUENCEFILE_NULL)));
          table.getParameters().remove(StorageConstants.SEQUENCEFILE_NULL);
        }
      } else {
        if (tableDesc.getMeta().getStoreType().equalsIgnoreCase(BuiltinStorages.PARQUET)) {
          sd.setInputFormat(parquet.hive.DeprecatedParquetInputFormat.class.getName());
          sd.setOutputFormat(parquet.hive.DeprecatedParquetOutputFormat.class.getName());
          sd.getSerdeInfo().setSerializationLib(parquet.hive.serde.ParquetHiveSerDe.class.getName());
        } else {
          throw new CatalogException(new NotImplementedException(tableDesc.getMeta().getStoreType()));
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
    HiveCatalogStoreClientPool.HiveCatalogStoreClient client = null;

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
    String partitionName = null;
    CatalogProtos.PartitionDescProto partitionDesc = null;

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
      case ADD_PARTITION:
        partitionName = alterTableDescProto.getPartitionDesc().getPartitionName();
        partitionDesc = getPartition(databaseName, tableName, partitionName);
        if(partitionDesc != null) {
          throw new AlreadyExistsPartitionException(databaseName, tableName, partitionName);
        }
        addPartition(databaseName, tableName, alterTableDescProto.getPartitionDesc());
        break;
      case DROP_PARTITION:
        partitionName = alterTableDescProto.getPartitionDesc().getPartitionName();
        partitionDesc = getPartition(databaseName, tableName, partitionName);
        if(partitionDesc == null) {
          throw new NoSuchPartitionException(databaseName, tableName, partitionName);
        }
        dropPartition(databaseName, tableName, partitionDesc);
        break;
      case SET_PROPERTY:
        // TODO - not implemented yet
        break;
      default:
        //TODO
    }
  }


  private void renameTable(String databaseName, String tableName, String newTableName) {
    HiveCatalogStoreClientPool.HiveCatalogStoreClient client = null;
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
    HiveCatalogStoreClientPool.HiveCatalogStoreClient client = null;
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
    HiveCatalogStoreClientPool.HiveCatalogStoreClient client = null;
    try {

      client = clientPool.getClient();
      Table table = client.getHiveClient().getTable(databaseName, tableName);
      List<FieldSchema> columns = table.getSd().getCols();
      columns.add(new FieldSchema(columnProto.getName(),
          HiveCatalogUtil.getHiveFieldType(columnProto.getDataType()), ""));
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

  private void addPartition(String databaseName, String tableName, CatalogProtos.PartitionDescProto
    partitionDescProto) {
    HiveCatalogStoreClientPool.HiveCatalogStoreClient client = null;
    try {

      client = clientPool.getClient();

      Partition partition = new Partition();
      partition.setDbName(databaseName);
      partition.setTableName(tableName);

      List<String> values = Lists.newArrayList();
      for(CatalogProtos.PartitionKeyProto keyProto : partitionDescProto.getPartitionKeysList()) {
        values.add(keyProto.getPartitionValue());
      }
      partition.setValues(values);

      Table table = client.getHiveClient().getTable(databaseName, tableName);
      StorageDescriptor sd = table.getSd();
      sd.setLocation(partitionDescProto.getPath());
      partition.setSd(sd);

      client.getHiveClient().add_partition(partition);
    } catch (Exception e) {
      throw new CatalogException(e);
    } finally {
      if (client != null) {
        client.release();
      }
    }
  }

  private void dropPartition(String databaseName, String tableName, CatalogProtos.PartitionDescProto
    partitionDescProto) {
    HiveCatalogStoreClientPool.HiveCatalogStoreClient client = null;
    try {

      client = clientPool.getClient();

      List<String> values = Lists.newArrayList();
      for(CatalogProtos.PartitionKeyProto keyProto : partitionDescProto.getPartitionKeysList()) {
        values.add(keyProto.getPartitionValue());
      }
      client.getHiveClient().dropPartition(databaseName, tableName, values, true);
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
  public List<CatalogProtos.PartitionDescProto> getPartitions(String databaseName,
                                                         String tableName) throws CatalogException {
    throw new UnsupportedOperationException();
  }


  @Override
  public CatalogProtos.PartitionDescProto getPartition(String databaseName, String tableName,
                                                       String partitionName) throws CatalogException {
    HiveCatalogStoreClientPool.HiveCatalogStoreClient client = null;
    CatalogProtos.PartitionDescProto.Builder builder = null;

    try {
      client = clientPool.getClient();

      Partition partition = client.getHiveClient().getPartition(databaseName, tableName, partitionName);
      builder = CatalogProtos.PartitionDescProto.newBuilder();
      builder.setPartitionName(partitionName);
      builder.setPath(partition.getSd().getLocation());

      String[] partitionNames = partitionName.split("/");

      for (int i = 0; i < partition.getValues().size(); i++) {
        String value = partition.getValues().get(i);
        CatalogProtos.PartitionKeyProto.Builder keyBuilder = CatalogProtos.PartitionKeyProto.newBuilder();

        String columnName = partitionNames[i].split("=")[0];
        keyBuilder.setColumnName(columnName);
        keyBuilder.setPartitionValue(value);
        builder.addPartitionKeys(keyBuilder);
      }
    } catch (NoSuchObjectException e) {
      return null;
    } catch (Exception e) {
      throw new CatalogException(e);
    } finally {
      if (client != null) {
        client.release();
      }
    }
    return builder.build();
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
    HiveCatalogStoreClientPool.HiveCatalogStoreClient client = null;

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

  @Override
  public List<ColumnProto> getAllColumns() throws CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<DatabaseProto> getAllDatabases() throws CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<IndexProto> getAllIndexes() throws CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<TablePartitionProto> getAllPartitions() throws CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<TableOptionProto> getAllTableOptions() throws CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<TableStatsProto> getAllTableStats() throws CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<TableDescriptorProto> getAllTables() throws CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<TablespaceProto> getTablespaces() throws CatalogException {
    throw new UnsupportedOperationException();
  }
}
