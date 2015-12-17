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
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.ql.io.StorageFormatDescriptor;
import org.apache.hadoop.hive.ql.io.StorageFormatFactory;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe;
import org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinarySerDe;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.tajo.BuiltinStorages;
import org.apache.tajo.TajoConstants;
import org.apache.tajo.algebra.Expr;
import org.apache.tajo.algebra.IsNullPredicate;
import org.apache.tajo.algebra.JsonHelper;
import org.apache.tajo.catalog.*;
import org.apache.tajo.catalog.partition.PartitionMethodDesc;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.catalog.proto.CatalogProtos.*;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.exception.*;
import org.apache.tajo.plan.expr.AlgebraicUtil;
import org.apache.tajo.plan.util.PartitionFilterAlgebraVisitor;
import org.apache.tajo.storage.StorageConstants;
import org.apache.tajo.util.KeyValueSet;
import org.apache.tajo.util.TUtil;
import org.apache.thrift.TException;
import parquet.hadoop.ParquetOutputFormat;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class HiveCatalogStore extends CatalogConstants implements CatalogStore {
  protected final Log LOG = LogFactory.getLog(getClass());

  private static String HIVE_WAREHOUSE_DIR_CONF_KEY = "hive.metastore.warehouse.dir";
  private static final int CLIENT_POOL_SIZE = 2;
  private static final StorageFormatFactory storageFormatFactory = new StorageFormatFactory();

  protected Configuration conf;
  private final HiveCatalogStoreClientPool clientPool;
  private final String defaultTableSpaceUri;
  private final String catalogUri;

  public HiveCatalogStore(final Configuration conf) {
    if (!(conf instanceof TajoConf)) {
      throw new TajoInternalError("Invalid Configuration Type:" + conf.getClass().getSimpleName());
    }
    this.conf = conf;
    this.defaultTableSpaceUri = TajoConf.getWarehouseDir((TajoConf) conf).toString();
    this.clientPool = new HiveCatalogStoreClientPool(CLIENT_POOL_SIZE, conf);
    this.catalogUri = conf.get(CATALOG_URI);
  }

  @Override
  public boolean existTable(final String databaseName, final String tableName) {
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
      throw new TajoInternalError(e);
    } finally {
      if (client != null) {
        client.release();
      }
    }

    return exist;
  }

  protected org.apache.hadoop.hive.ql.metadata.Table getHiveTable(String databaseName, final String tableName)
      throws UndefinedTableException {

    HiveCatalogStoreClientPool.HiveCatalogStoreClient client = null;
    try {
      client = clientPool.getClient();
      return HiveCatalogUtil.getTable(client.getHiveClient(), databaseName, tableName);
    } catch (NoSuchObjectException nsoe) {
      throw new UndefinedTableException(tableName);
    } catch (Exception e) {
      throw new TajoInternalError(e);
    } finally {
      if (client != null) client.release();
    }
  }

  @Override
  public final CatalogProtos.TableDescProto getTable(String databaseName, final String tableName)
      throws UndefinedTableException {

    org.apache.hadoop.hive.ql.metadata.Table table = null;
    Path path = null;
    String dataFormat = null;
    org.apache.tajo.catalog.Schema schema = null;
    KeyValueSet options = null;
    TableStats stats = null;
    PartitionMethodDesc partitions = null;

    //////////////////////////////////
    // set tajo table schema.
    //////////////////////////////////
    try {
      // get hive table schema
      table = getHiveTable(databaseName, tableName);
      path = table.getPath();

      // convert HiveCatalogStore field schema into tajo field schema.
      schema = new org.apache.tajo.catalog.Schema();

      List<FieldSchema> fieldSchemaList = table.getCols();
      boolean isPartitionKey;
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


        dataFormat = HiveCatalogUtil.getDataFormat(table.getSd());
        if (BuiltinStorages.TEXT.equals(dataFormat)) {
          options.set(StorageConstants.TEXT_DELIMITER, StringEscapeUtils.escapeJava(fieldDelimiter));
          options.set(StorageConstants.TEXT_NULL, StringEscapeUtils.escapeJava(nullFormat));

        } else if (BuiltinStorages.RCFILE.equals(dataFormat)) {
          options.set(StorageConstants.RCFILE_NULL, StringEscapeUtils.escapeJava(nullFormat));
          String serde = properties.getProperty(serdeConstants.SERIALIZATION_LIB);
          if (LazyBinaryColumnarSerDe.class.getName().equals(serde)) {
            options.set(StorageConstants.RCFILE_SERDE, StorageConstants.DEFAULT_BINARY_SERDE);
          } else if (ColumnarSerDe.class.getName().equals(serde)) {
            options.set(StorageConstants.RCFILE_SERDE, StorageConstants.DEFAULT_TEXT_SERDE);
          }

        } else if (BuiltinStorages.SEQUENCE_FILE.equals(dataFormat)) {
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
          stats.setNumBytes(totalSize);
        }
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
    } catch (Throwable t) {
      throw new TajoInternalError(t);
    }

    TableMeta meta = new TableMeta(dataFormat, options);
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
  public final List<String> getAllTableNames(String databaseName) {
    HiveCatalogStoreClientPool.HiveCatalogStoreClient client = null;

    try {
      client = clientPool.getClient();
      return client.getHiveClient().getAllTables(databaseName);
    } catch (TException e) {
      throw new TajoInternalError(e);
    } finally {
      if(client != null) client.release();
    }
  }

  @Override
  public String getUri() {
    return catalogUri;
  }

  @Override
  public void createTablespace(String spaceName, String spaceUri) {
    // SKIP
  }

  @Override
  public boolean existTablespace(String spaceName) {
    // SKIP
    return spaceName.equals(TajoConstants.DEFAULT_TABLESPACE_NAME);
  }

  @Override
  public void dropTablespace(String spaceName) {
    // SKIP
  }

  @Override
  public Collection<String> getAllTablespaceNames() {
    return Lists.newArrayList(TajoConstants.DEFAULT_TABLESPACE_NAME);
  }

  @Override
  public TablespaceProto getTablespace(String spaceName) {
    if (spaceName.equals(TajoConstants.DEFAULT_TABLESPACE_NAME)) {
      TablespaceProto.Builder builder = TablespaceProto.newBuilder();
      builder.setSpaceName(TajoConstants.DEFAULT_TABLESPACE_NAME);
      builder.setUri(defaultTableSpaceUri);
      return builder.build();
    } else {
      throw new TajoRuntimeException(new UnsupportedException("Tablespace in HiveMeta"));
    }
  }

  @Override
  public void updateTableStats(CatalogProtos.UpdateTableStatsProto statsProto) {
    // TODO - not implemented yet
  }

  @Override
  public void alterTablespace(CatalogProtos.AlterTablespaceProto alterProto) {
    // SKIP
  }

  @Override
  public void createDatabase(String databaseName, String tablespaceName) throws DuplicateDatabaseException {
    HiveCatalogStoreClientPool.HiveCatalogStoreClient client = null;

    try {
      Database database = new Database(
          databaseName,
          "",
          defaultTableSpaceUri + "/" + databaseName,
              new HashMap<>());
      client = clientPool.getClient();
      client.getHiveClient().createDatabase(database);
    } catch (AlreadyExistsException e) {
      throw new DuplicateDatabaseException(databaseName);
    } catch (Throwable t) {
      throw new TajoInternalError(t);
    } finally {
      if (client != null) {
        client.release();
      }
    }
  }

  @Override
  public boolean existDatabase(String databaseName) {
    HiveCatalogStoreClientPool.HiveCatalogStoreClient client = null;

    try {
      client = clientPool.getClient();
      List<String> databaseNames = client.getHiveClient().getAllDatabases();
      return databaseNames.contains(databaseName);
    } catch (Throwable t) {
      throw new TajoInternalError(t);
    } finally {
      if (client != null) {
        client.release();
      }
    }
  }

  @Override
  public void dropDatabase(String databaseName) throws UndefinedDatabaseException {
    HiveCatalogStoreClientPool.HiveCatalogStoreClient client = null;

    try {
      client = clientPool.getClient();
      client.getHiveClient().dropDatabase(databaseName);
    } catch (NoSuchObjectException e) {
      throw new UndefinedDatabaseException(databaseName);
    } catch (Throwable t) {
      throw new  TajoInternalError(t);
    } finally {
      if (client != null) {
        client.release();
      }
    }
  }

  @Override
  public Collection<String> getAllDatabaseNames() {
    HiveCatalogStoreClientPool.HiveCatalogStoreClient client = null;

    try {
      client = clientPool.getClient();
      return client.getHiveClient().getAllDatabases();
    } catch (TException e) {
      throw new TajoInternalError(e);
    } finally {
      if (client != null) {
        client.release();
      }
    }
  }

  @Override
  public final void createTable(final CatalogProtos.TableDescProto tableDescProto) {
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
      table.setParameters(new HashMap<>(tableDesc.getMeta().getPropertySet().getAllKeyValus()));
      // TODO: set owner
      //table.setOwner();

      StorageDescriptor sd = new StorageDescriptor();
      sd.setSerdeInfo(new SerDeInfo());
      sd.getSerdeInfo().setParameters(new HashMap<>());
      sd.getSerdeInfo().setName(table.getTableName());

      //If tableType is a managed-table, the location is hive-warehouse dir
      // and it will be wrong path in output committing
      table.setTableType(TableType.EXTERNAL_TABLE.name());
      table.putToParameters("EXTERNAL", "TRUE");

      Path tablePath = new Path(tableDesc.getUri());
      FileSystem fs = tablePath.getFileSystem(conf);
      if (fs.isFile(tablePath)) {
        LOG.warn("A table path is a file, but HiveCatalogStore does not allow a file path.");
        sd.setLocation(tablePath.getParent().toString());
      } else {
        sd.setLocation(tablePath.toString());
      }

      // set column information
      List<Column> columns = tableDesc.getSchema().getRootColumns();
      ArrayList<FieldSchema> cols = new ArrayList<>(columns.size());

      for (Column eachField : columns) {
        cols.add(new FieldSchema(eachField.getSimpleName(),
            HiveCatalogUtil.getHiveFieldType(eachField.getDataType()), ""));
      }
      sd.setCols(cols);

      // set partition keys
      if (tableDesc.hasPartition() && tableDesc.getPartitionMethod().getPartitionType().equals(PartitionType.COLUMN)) {
        List<FieldSchema> partitionKeys = new ArrayList<>();
        for (Column eachPartitionKey : tableDesc.getPartitionMethod().getExpressionSchema().getRootColumns()) {
          partitionKeys.add(new FieldSchema(eachPartitionKey.getSimpleName(),
              HiveCatalogUtil.getHiveFieldType(eachPartitionKey.getDataType()), ""));
        }
        table.setPartitionKeys(partitionKeys);
      }

      if (tableDesc.getMeta().getDataFormat().equalsIgnoreCase(BuiltinStorages.RCFILE)) {
        StorageFormatDescriptor descriptor = storageFormatFactory.get(IOConstants.RCFILE);
        sd.setInputFormat(descriptor.getInputFormat());
        sd.setOutputFormat(descriptor.getOutputFormat());

        String serde = tableDesc.getMeta().getProperty(StorageConstants.RCFILE_SERDE);
        if (StorageConstants.DEFAULT_TEXT_SERDE.equals(serde)) {
          sd.getSerdeInfo().setSerializationLib(ColumnarSerDe.class.getName());
        } else {
          sd.getSerdeInfo().setSerializationLib(LazyBinaryColumnarSerDe.class.getName());
        }

        if (tableDesc.getMeta().getPropertySet().containsKey(StorageConstants.RCFILE_NULL)) {
          table.putToParameters(serdeConstants.SERIALIZATION_NULL_FORMAT,
              StringEscapeUtils.unescapeJava(tableDesc.getMeta().getProperty(StorageConstants.RCFILE_NULL)));
        }
      } else if (tableDesc.getMeta().getDataFormat().equals(BuiltinStorages.TEXT)) {
        // TextFileStorageFormatDescriptor has deprecated class. so the class name set directly
        sd.setInputFormat(TextInputFormat.class.getName());
        sd.setOutputFormat(HiveIgnoreKeyTextOutputFormat.class.getName());
        sd.getSerdeInfo().setSerializationLib(LazySimpleSerDe.class.getName());

        String fieldDelimiter = tableDesc.getMeta().getProperty(StorageConstants.TEXT_DELIMITER,
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

        if (tableDesc.getMeta().containsProperty(StorageConstants.TEXT_NULL)) {
          table.putToParameters(serdeConstants.SERIALIZATION_NULL_FORMAT,
              StringEscapeUtils.unescapeJava(tableDesc.getMeta().getProperty(StorageConstants.TEXT_NULL)));
          table.getParameters().remove(StorageConstants.TEXT_NULL);
        }
      } else if (tableDesc.getMeta().getDataFormat().equalsIgnoreCase(BuiltinStorages.SEQUENCE_FILE)) {
        StorageFormatDescriptor descriptor = storageFormatFactory.get(IOConstants.SEQUENCEFILE);
        sd.setInputFormat(descriptor.getInputFormat());
        sd.setOutputFormat(descriptor.getOutputFormat());

        String serde = tableDesc.getMeta().getProperty(StorageConstants.SEQUENCEFILE_SERDE);

        if (StorageConstants.DEFAULT_TEXT_SERDE.equals(serde)) {
          sd.getSerdeInfo().setSerializationLib(LazySimpleSerDe.class.getName());

          String fieldDelimiter = tableDesc.getMeta().getProperty(StorageConstants.SEQUENCEFILE_DELIMITER,
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
          sd.getSerdeInfo().setSerializationLib(LazyBinarySerDe.class.getName());
        }

        if (tableDesc.getMeta().containsProperty(StorageConstants.SEQUENCEFILE_NULL)) {
          table.putToParameters(serdeConstants.SERIALIZATION_NULL_FORMAT,
              StringEscapeUtils.unescapeJava(tableDesc.getMeta().getProperty(StorageConstants.SEQUENCEFILE_NULL)));
          table.getParameters().remove(StorageConstants.SEQUENCEFILE_NULL);
        }
      } else if (tableDesc.getMeta().getDataFormat().equalsIgnoreCase(BuiltinStorages.PARQUET)) {
        StorageFormatDescriptor descriptor = storageFormatFactory.get(IOConstants.PARQUET);
        sd.setInputFormat(descriptor.getInputFormat());
        sd.setOutputFormat(descriptor.getOutputFormat());
        sd.getSerdeInfo().setSerializationLib(descriptor.getSerde());

        if (tableDesc.getMeta().containsProperty(ParquetOutputFormat.COMPRESSION)) {
          table.putToParameters(ParquetOutputFormat.COMPRESSION,
              tableDesc.getMeta().getProperty(ParquetOutputFormat.COMPRESSION));
        }
      } else {
        throw new UnsupportedException(tableDesc.getMeta().getDataFormat() + " in HivecatalogStore");
      }

      sd.setSortCols(new ArrayList<>());

      table.setSd(sd);
      client.getHiveClient().createTable(table);
    } catch (Throwable t) {
      throw new TajoInternalError(t);
    } finally {
      if(client != null) client.release();
    }
  }

  @Override
  public final void dropTable(String databaseName, final String tableName) {
    HiveCatalogStoreClientPool.HiveCatalogStoreClient client = null;

    try {
      client = clientPool.getClient();
      client.getHiveClient().dropTable(databaseName, tableName, false, false);
    } catch (NoSuchObjectException nsoe) {
    } catch (Exception e) {
      throw new TajoInternalError(e);
    } finally {
      if (client != null) {
        client.release();
      }
    }
  }


  @Override
  public void alterTable(final CatalogProtos.AlterTableDescProto alterTableDescProto)
      throws DuplicateTableException, DuplicateColumnException, DuplicatePartitionException,
      UndefinedPartitionException {

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
          throw new DuplicateTableException(alterTableDescProto.getNewTableName());
        }
        renameTable(databaseName, tableName, alterTableDescProto.getNewTableName().toLowerCase());
        break;
      case RENAME_COLUMN:
        if (existColumn(databaseName,tableName, alterTableDescProto.getAlterColumnName().getNewColumnName())) {
          throw new DuplicateColumnException(alterTableDescProto.getAlterColumnName().getNewColumnName());
        }
        renameColumn(databaseName, tableName, alterTableDescProto.getAlterColumnName());
        break;
      case ADD_COLUMN:
        if (existColumn(databaseName,tableName, alterTableDescProto.getAddColumn().getName())) {
          throw new DuplicateColumnException(alterTableDescProto.getAddColumn().getName());
        }
        addNewColumn(databaseName, tableName, alterTableDescProto.getAddColumn());
        break;
      case ADD_PARTITION:
        partitionName = alterTableDescProto.getPartitionDesc().getPartitionName();
        partitionDesc = getPartition(databaseName, tableName, partitionName);
        if(partitionDesc != null) {
          throw new DuplicatePartitionException(partitionName);
        }
        addPartition(databaseName, tableName, alterTableDescProto.getPartitionDesc());
        break;
      case DROP_PARTITION:
        partitionName = alterTableDescProto.getPartitionDesc().getPartitionName();
        partitionDesc = getPartition(databaseName, tableName, partitionName);
        if(partitionDesc == null) {
          throw new UndefinedPartitionException(partitionName);
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
      throw new TajoInternalError(e);
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
      throw new TajoInternalError(e);
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
      throw new TajoInternalError(e);
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

      Map<String, String> params = new HashMap<>();
      params.put(StatsSetupConst.TOTAL_SIZE, Long.toString(partitionDescProto.getNumBytes()));
      partition.setParameters(params);

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
      throw new TajoInternalError(e);
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
      throw new TajoInternalError(e);
    } finally {
      if (client != null) {
        client.release();
      }
    }
  }

  @Override
  public CatalogProtos.PartitionMethodProto getPartitionMethod(String databaseName, String tableName)
      throws UndefinedTableException, UndefinedPartitionMethodException {

    org.apache.hadoop.hive.ql.metadata.Table table;
    HiveCatalogStoreClientPool.HiveCatalogStoreClient client = null;

    PartitionMethodDesc partitionMethodDesc = null;
    try {
      try {
        client = clientPool.getClient();
        table = HiveCatalogUtil.getTable(client.getHiveClient(), databaseName, tableName);
      } catch (NoSuchObjectException nsoe) {
        throw new UndefinedTableException(tableName);
      } catch (Exception e) {
        throw new TajoInternalError(e);
      }

      // set partition keys
      List<FieldSchema> partitionKeys = table.getPartitionKeys();

      if (partitionKeys != null && partitionKeys.size() > 0) {
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
          partitionMethodDesc = new PartitionMethodDesc(
              databaseName,
              tableName,
              PartitionType.COLUMN,
              sb.toString(),
              expressionSchema);
        }
      } else {
        throw new UndefinedPartitionMethodException(tableName);
      }
    } catch (Throwable t) {
      throw new TajoInternalError(t);
    } finally {
      if(client != null) client.release();
    }

    return partitionMethodDesc.getProto();
  }

  @Override
  public boolean existPartitionMethod(String databaseName, String tableName) throws UndefinedTableException {
    boolean exist = false;
    org.apache.hadoop.hive.ql.metadata.Table table;
    HiveCatalogStoreClientPool.HiveCatalogStoreClient client = null;

    try {
      try {
        client = clientPool.getClient();
        table = HiveCatalogUtil.getTable(client.getHiveClient(), databaseName, tableName);
      } catch (NoSuchObjectException nsoe) {
        throw new UndefinedTableException(tableName);
      } catch (Exception e) {
        throw new TajoInternalError(e);
      }

      // set partition keys
      List<FieldSchema> partitionKeys = table.getPartitionKeys();

      if (partitionKeys != null && partitionKeys.size() > 0) {
        exist = true;
      }
    } finally {
      if(client != null) client.release();
    }

    return exist;
  }

  @Override
  public boolean existPartitions(String databaseName, String tableName) throws UndefinedDatabaseException,
    UndefinedTableException, UndefinedPartitionMethodException {

    HiveCatalogStoreClientPool.HiveCatalogStoreClient client = null;
    boolean result = false;

    try {
      client = clientPool.getClient();
      List<Partition> partitions = client.getHiveClient().listPartitionsByFilter(databaseName, tableName,
        "", (short) -1);

      if (partitions.size() > 0) {
        result = true;
      }
    } catch (Exception e) {
      throw new TajoInternalError(e);
    } finally {
      if (client != null) {
        client.release();
      }
    }

    return result;
  }

  @Override
  public List<CatalogProtos.PartitionDescProto> getPartitionsOfTable(String databaseName, String tableName)
      throws UndefinedDatabaseException, UndefinedTableException, UndefinedPartitionMethodException {
    List<PartitionDescProto> list = null;

    try {
      if (!existDatabase(databaseName)) {
        throw new UndefinedDatabaseException(tableName);
      }

      if (!existTable(databaseName, tableName)) {
        throw new UndefinedTableException(tableName);
      }

      if (!existPartitionMethod(databaseName, tableName)) {
        throw new UndefinedPartitionMethodException(tableName);
      }

      list = getPartitionsFromHiveMetaStore(databaseName, tableName, "");
    } catch (Exception se) {
      throw new TajoInternalError(se);
    }

    return list;
  }

  @Override
  public List<PartitionDescProto> getPartitionsByAlgebra(PartitionsByAlgebraProto request) throws
    UndefinedDatabaseException, UndefinedTableException, UndefinedPartitionMethodException, UnsupportedException {

    List<PartitionDescProto> list = null;

    try {
      String databaseName = request.getDatabaseName();
      String tableName = request.getTableName();

      if (!existDatabase(databaseName)) {
        throw new UndefinedDatabaseException(tableName);
      }

      if (!existTable(databaseName, tableName)) {
        throw new UndefinedTableException(tableName);
      }

      if (!existPartitionMethod(databaseName, tableName)) {
        throw new UndefinedPartitionMethodException(tableName);
      }

      TableDescProto tableDesc = getTable(databaseName, tableName);
      String filter = getFilter(databaseName, tableName, tableDesc.getPartition().getExpressionSchema().getFieldsList()
        , request.getAlgebra());
      list = getPartitionsFromHiveMetaStore(databaseName, tableName, filter);
    } catch (UnsupportedException ue) {
      throw ue;
    } catch (Exception se) {
      throw new TajoInternalError(se);
    }

    return list;
  }

  private String getFilter(String databaseName, String tableName, List<ColumnProto> partitionColumns
      , String json) throws TajoException {

    Expr[] exprs = null;

    if (json != null && !json.isEmpty()) {
      Expr algebra = JsonHelper.fromJson(json, Expr.class);
      exprs = AlgebraicUtil.toConjunctiveNormalFormArray(algebra);
    }

    PartitionFilterAlgebraVisitor visitor = new PartitionFilterAlgebraVisitor();
    visitor.setIsHiveCatalog(true);

    Expr[] filters = AlgebraicUtil.getRearrangedCNFExpressions(databaseName + "." + tableName, partitionColumns, exprs);

    StringBuffer sb = new StringBuffer();

    // Write join clause from second column to last column.
    Column target;

    String result;
    for (int i = 0; i < partitionColumns.size(); i++) {
      target = new Column(partitionColumns.get(i));

      if (!(filters[i] instanceof IsNullPredicate)) {
        visitor.setColumn(target);
        visitor.visit(null, new Stack<>(), filters[i]);
        result = visitor.getResult();

        // If visitor build filter successfully, add filter to be used for executing hive api.
        if (result.length() > 0) {
          if (sb.length() > 0) {
            sb.append(" AND ");
          }
          sb.append(" ( ").append(result).append(" ) ");
        } else {
          throw new TajoInternalError("Filter does not exist : " + filters[i].toJson());
        }
      }
    }

    return sb.toString();
  }

  /**
   * Get list of partitions matching specified filter.
   *
   * For example, consider you have a partitioned table for three columns (i.e., col1, col2, col3).
   * Assume that an user want to give a condition WHERE (col1 ='1' or col1 = '100') and col3 > 20 .
   *
   * Then, the filter string would be written as following:
   *   (col1 =\"1\" or col1 = \"100\") and col3 > 20
   *
   *
   * @param databaseName
   * @param tableName
   * @param filter
   * @return
   */
  private List<PartitionDescProto> getPartitionsFromHiveMetaStore(String databaseName, String tableName,
                                                                         String filter) {
    HiveCatalogStoreClientPool.HiveCatalogStoreClient client = null;
    List<PartitionDescProto> partitions = null;
    TableDescProto tableDesc = null;
    List<ColumnProto> parititonColumns = null;

    try {
      partitions = TUtil.newList();
      client = clientPool.getClient();

      List<Partition> hivePartitions = client.getHiveClient().listPartitionsByFilter(databaseName, tableName
        , filter, (short) -1);

      tableDesc = getTable(databaseName, tableName);
      parititonColumns = tableDesc.getPartition().getExpressionSchema().getFieldsList();

      StringBuilder partitionName = new StringBuilder();
      for (Partition hivePartition : hivePartitions) {
        CatalogProtos.PartitionDescProto.Builder builder = CatalogProtos.PartitionDescProto.newBuilder();
        builder.setPath(hivePartition.getSd().getLocation());

        partitionName.delete(0, partitionName.length());
        for (int i = 0; i < parititonColumns.size(); i++) {
          if (i > 0) {
            partitionName.append(File.separator);
          }
          partitionName.append(CatalogUtil.extractSimpleName(parititonColumns.get(i).getName()));
          partitionName.append("=");
          partitionName.append(hivePartition.getValues().get(i));
        }

        builder.setPartitionName(partitionName.toString());

        Map<String, String> params = hivePartition.getParameters();
        if (params != null) {
          if (params.get(StatsSetupConst.TOTAL_SIZE) != null) {
            builder.setNumBytes(Long.parseLong(params.get(StatsSetupConst.TOTAL_SIZE)));
          }
        }

        partitions.add(builder.build());
      }
    } catch (Exception e) {
      throw new TajoInternalError(e);
    } finally {
      if (client != null) {
        client.release();
      }
    }

    return partitions;
  }

  @Override
  public CatalogProtos.PartitionDescProto getPartition(String databaseName, String tableName,
                                                       String partitionName) {
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

        Map<String, String> params = partition.getParameters();
        if (params != null) {
          if (params.get(StatsSetupConst.TOTAL_SIZE) != null) {
            builder.setNumBytes(Long.parseLong(params.get(StatsSetupConst.TOTAL_SIZE)));
          }
        }

      }
    } catch (NoSuchObjectException e) {
      return null;
    } catch (Exception e) {
      throw new TajoInternalError(e);
    } finally {
      if (client != null) {
        client.release();
      }
    }
    return builder.build();
  }

  @Override
  public final void addFunction(final FunctionDesc func) {
    // TODO - not implemented yet
    throw new UnsupportedOperationException();
  }

  @Override
  public final void deleteFunction(final FunctionDesc func) {
    // TODO - not implemented yet
    throw new UnsupportedOperationException();
  }

  @Override
  public final void existFunction(final FunctionDesc func) {
    // TODO - not implemented yet
    throw new UnsupportedOperationException();
  }

  @Override
  public final List<String> getAllFunctionNames() {
    // TODO - not implemented yet
    throw new UnsupportedOperationException();
  }

  @Override
  public void createIndex(CatalogProtos.IndexDescProto proto) {
    // TODO - not implemented yet
    throw new UnsupportedOperationException();
  }

  @Override
  public void dropIndex(String databaseName, String indexName) {
    // TODO - not implemented yet
    throw new UnsupportedOperationException();
  }

  @Override
  public CatalogProtos.IndexDescProto getIndexByName(String databaseName, String indexName) {
    // TODO - not implemented yet
    throw new UnsupportedOperationException();
  }

  @Override
  public CatalogProtos.IndexDescProto getIndexByColumns(String databaseName, String tableName, String[] columnNames)
      {
    // TODO - not implemented yet
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean existIndexByName(String databaseName, String indexName) {
    // TODO - not implemented yet
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean existIndexByColumns(String databaseName, String tableName, String[] columnNames)
      {
    // TODO - not implemented yet
    throw new UnsupportedOperationException();
  }

  @Override
  public List<String> getAllIndexNamesByTable(String databaseName, String tableName) {
    // TODO - not implemented yet
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean existIndexesByTable(String databaseName, String tableName) {
    // TODO - not implemented yet
    throw new UnsupportedOperationException();
  }

  @Override
  public final void close() {
    clientPool.close();
  }

  private boolean existColumn(final String databaseName ,final String tableName , final String columnName) {
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
      throw new TajoInternalError(e);
    } finally {
      if (client != null) {
        client.release();
      }
    }

    return exist;
  }

  @Override
  public List<ColumnProto> getAllColumns() {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<DatabaseProto> getAllDatabases() {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<IndexDescProto> getAllIndexes() {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<TablePartitionProto> getAllPartitions() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void addPartitions(String databaseName, String tableName, List<CatalogProtos.PartitionDescProto> partitions
    , boolean ifNotExists) {
    HiveCatalogStoreClientPool.HiveCatalogStoreClient client = null;
    List<Partition> addPartitions = TUtil.newList();
    CatalogProtos.PartitionDescProto existingPartition = null;

    try {
      client = clientPool.getClient();
      for (CatalogProtos.PartitionDescProto partitionDescProto : partitions) {
        existingPartition = getPartition(databaseName, tableName, partitionDescProto.getPartitionName());

        // Unfortunately, hive client add_partitions doesn't run as expected. The method never read the ifNotExists
        // parameter. So, if Tajo adds existing partition to Hive, it will threw AlreadyExistsException. To avoid
        // above error, we need to filter existing partitions before call add_partitions.
        if (existingPartition == null) {
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

          addPartitions.add(partition);
        }
      }

      if (addPartitions.size() > 0) {
        client.getHiveClient().add_partitions(addPartitions, true, true);
      }
    } catch (Exception e) {
      throw new TajoInternalError(e);
    } finally {
      if (client != null) {
        client.release();
      }
    }

  }

  @Override
  public List<TableOptionProto> getAllTableProperties() {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<TableStatsProto> getAllTableStats() {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<TableDescriptorProto> getAllTables() {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<TablespaceProto> getTablespaces() {
    return Lists.newArrayList(getTablespace(TajoConstants.DEFAULT_TABLESPACE_NAME));
  }
}
