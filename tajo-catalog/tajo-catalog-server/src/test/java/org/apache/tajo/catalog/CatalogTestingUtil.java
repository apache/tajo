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

package org.apache.tajo.catalog;

import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.TajoConstants;
import org.apache.tajo.annotation.NotNull;
import org.apache.tajo.annotation.Nullable;
import org.apache.tajo.catalog.partition.PartitionDesc;
import org.apache.tajo.catalog.partition.PartitionMethodDesc;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.catalog.proto.CatalogProtos.IndexMethod;
import org.apache.tajo.catalog.proto.CatalogProtos.PartitionKeyProto;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.catalog.store.*;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.exception.UnsupportedCatalogStore;
import org.apache.tajo.util.KeyValueSet;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.apache.tajo.TajoConstants.DEFAULT_DATABASE_NAME;

public class CatalogTestingUtil {

  public static TajoConf configureCatalog(TajoConf conf, String testDirPath) throws UnsupportedCatalogStore {

    String driverClassName = System.getProperty(CatalogConstants.STORE_CLASS);
    final boolean useDefaultCatalog = driverClassName == null;

    if (useDefaultCatalog) {
      conf = initializeDerbyStore(conf, testDirPath);

    } else {
      Class clazz;
      try {
        clazz = Class.forName(driverClassName);
      } catch (ClassNotFoundException e) {
        throw new UnsupportedCatalogStore(driverClassName);
      }
      Class<? extends CatalogStore> catalogClass = clazz;

      String catalogURI = System.getProperty(CatalogConstants.CATALOG_URI);
      if (catalogURI == null) {
        catalogURI = getCatalogURI(catalogClass, null, testDirPath);
      }

      configureCatalogClassAndUri(conf, catalogClass, catalogURI);

      if (requireAuth(catalogClass)) {
        String connectionId = System.getProperty(CatalogConstants.CONNECTION_ID);
        String connectionPasswd = System.getProperty(CatalogConstants.CONNECTION_PASSWORD);

        assert connectionId != null;
        conf.set(CatalogConstants.CONNECTION_ID, connectionId);
        if (connectionPasswd != null) {
          conf.set(CatalogConstants.CONNECTION_PASSWORD, connectionPasswd);
        }
      }
    }
    return conf;
  }

  static <T extends CatalogStore> boolean requireAuth(Class<T> clazz) {
    return clazz.equals(MySQLStore.class) ||
        clazz.equals(MariaDBStore.class) ||
        clazz.equals(PostgreSQLStore.class) ||
        clazz.equals(OracleStore.class);
  }

  private static TajoConf initializeDerbyStore(TajoConf conf, String testDirPath) throws UnsupportedCatalogStore {
    return configureCatalogClassAndUri(conf, DerbyStore.class, getInmemoryDerbyCatalogURI(testDirPath));
  }

  private static <T extends CatalogStore> TajoConf configureCatalogClassAndUri(TajoConf conf,
                                                                               Class<T> catalogClass,
                                                                               String catalogUri) {
    conf.set(CatalogConstants.STORE_CLASS, catalogClass.getCanonicalName());
    conf.set(CatalogConstants.CATALOG_URI, catalogUri);
    conf.setVar(ConfVars.CATALOG_ADDRESS, "localhost:0");
    return conf;
  }

  private static String getInmemoryDerbyCatalogURI(String testDirPath) throws UnsupportedCatalogStore {
    return getCatalogURI(DerbyStore.class, "memory", testDirPath);
  }

  private static <T extends CatalogStore> String getCatalogURI(@NotNull Class<T> clazz,
                                                               @Nullable String schemeSpecificPart,
                                                               @NotNull String testDirPath)
      throws UnsupportedCatalogStore {
    String uriScheme = getCatalogURIScheme(clazz);
    StringBuilder sb = new StringBuilder("jdbc:").append(uriScheme).append(":");
    if (schemeSpecificPart != null) {
      sb.append(schemeSpecificPart).append(":");
    }
    sb.append(testDirPath).append("/db;create=true");
    return sb.toString();
  }

  private static <T extends CatalogStore> String getCatalogURIScheme(Class<T> clazz) throws UnsupportedCatalogStore {
    if (clazz.equals(DerbyStore.class)) {
      return "derby";
    } else if (clazz.equals(MariaDBStore.class)) {
      return "mariadb";
    } else if (clazz.equals(MySQLStore.class)) {
      return "mysql";
    } else if (clazz.equals(OracleStore.class)) {
      return "oracle";
    } else if (clazz.equals(PostgreSQLStore.class)) {
      return "postgresql";
    } else {
      throw new UnsupportedCatalogStore(clazz.getCanonicalName());
    }
  }

  public static PartitionDesc buildPartitionDesc(String partitionName) {
    PartitionDesc partitionDesc = new PartitionDesc();
    partitionDesc.setPartitionName(partitionName);

    String[] partitionNames = partitionName.split("/");

    List<PartitionKeyProto> partitionKeyList = new ArrayList<>();
    for(int i = 0; i < partitionNames.length; i++) {
      String [] splits = partitionNames[i].split("=");
      String columnName = "", partitionValue = "";
      if (splits.length == 2) {
        columnName = splits[0];
        partitionValue = splits[1];
      } else if (splits.length == 1) {
        if (partitionNames[i].charAt(0) == '=') {
          partitionValue = splits[0];
        } else {
          columnName = "";
        }
      }

      PartitionKeyProto.Builder builder = PartitionKeyProto.newBuilder();
      builder.setColumnName(partitionValue);
      builder.setPartitionValue(columnName);
      partitionKeyList.add(builder.build());
    }

    partitionDesc.setPartitionKeys(partitionKeyList);

    partitionDesc.setPath("hdfs://xxx.com/warehouse/" + partitionName);
    return partitionDesc;
  }

  public static void prepareBaseData(CatalogService catalog, String testDir) throws Exception {
    catalog.createTablespace("space1", "hdfs://xxx.com/warehouse");
    catalog.createTablespace("SpAcE1", "hdfs://xxx.com/warehouse");

    catalog.createDatabase("TestDatabase1", "space1");
    catalog.createDatabase("testDatabase1", "SpAcE1");

    catalog.createTable(buildTableDesc("TestDatabase1", "TestTable1", testDir));
    catalog.createTable(buildTableDesc("TestDatabase1", "testTable1", testDir));
    catalog.createTable(buildPartitionTableDesc("TestDatabase1", "TestPartition1", testDir));
    catalog.createTable(buildPartitionTableDesc("TestDatabase1", "testPartition1", testDir));
  }

  public static void cleanupBaseData(CatalogService catalog) throws Exception {
    catalog.dropTable(CatalogUtil.buildFQName("TestDatabase1", "testPartition1"));
    catalog.dropTable(CatalogUtil.buildFQName("TestDatabase1", "TestPartition1"));
    catalog.dropTable(CatalogUtil.buildFQName("TestDatabase1", "TestTable1"));
    catalog.dropTable(CatalogUtil.buildFQName("TestDatabase1", "testTable1"));

    catalog.dropDatabase("TestDatabase1");
    catalog.dropDatabase("testDatabase1");

    catalog.dropTablespace("space1");
    catalog.dropTablespace("SpAcE1");
  }

  public static TableDesc buildTableDesc(String databaseName, String tableName, String testDir) throws IOException {
    Schema schema = new Schema();
    schema.addColumn(CatalogUtil.buildFQName(tableName, "Column"), Type.BLOB);
    schema.addColumn(CatalogUtil.buildFQName(tableName, "column"), Type.INT4);
    schema.addColumn(CatalogUtil.buildFQName(tableName, "cOlumn"), Type.INT8);
    Path path = new Path(testDir + "/" + UUID.randomUUID().toString(), tableName);
    TableDesc desc = new TableDesc(
        CatalogUtil.buildFQName(databaseName, tableName),
        schema,
        new TableMeta("TEXT", new KeyValueSet()),
        path.toUri(), true);
    desc.setStats(buildTableStats());
    desc.getMeta().setOptions(buildOptions());
    return desc;
  }

  public static TableDesc buildPartitionTableDesc(String databaseName, String tableName, String testDir) throws Exception {
    Schema partSchema = new Schema();
    partSchema.addColumn(CatalogUtil.buildFQName(tableName, "DaTe"), Type.TEXT);
    partSchema.addColumn(CatalogUtil.buildFQName(tableName, "dAtE"), Type.TEXT);
    PartitionMethodDesc partitionMethodDesc =
        new PartitionMethodDesc(DEFAULT_DATABASE_NAME, tableName,
            CatalogProtos.PartitionType.COLUMN, "id,name", partSchema);
    TableDesc desc = buildTableDesc(databaseName, tableName, testDir);
    desc.setPartitionMethod(partitionMethodDesc);
    return desc;
  }

  public static TableStats buildTableStats() {
    TableStats stats = new TableStats();
    stats.setAvgRows(1000);
    stats.setNumBlocks(100);
    stats.setNumBytes(10000);
    stats.setNumRows(5000);
    stats.setNumShuffleOutputs(40);
    stats.setReadBytes(200);
    return stats;
  }

  public static KeyValueSet buildOptions() {
    KeyValueSet options = new KeyValueSet();
    options.set("testString", "ThisIsTest");
    options.setBool("testBool", true);
    options.setFloat("testFloat", 0.2f);
    options.setInt("testInt", 60);
    options.setLong("testLong", 800l);
    return options;
  }

  public static IndexDesc buildIndexDescs(String databaseName, String indexName, TableDesc table, Column... cols)
      throws IOException, URISyntaxException {
    Preconditions.checkArgument(cols.length > 0);
    SortSpec[] colSpecs = new SortSpec[cols.length];
    for (int i = 0; i < cols.length; i++) {
      colSpecs[i] = new SortSpec(cols[i], true, false);
    }
    return new IndexDesc(databaseName, CatalogUtil.extractSimpleName(table.getName()),
        indexName, new URI("idx_test"), colSpecs,
        IndexMethod.TWO_LEVEL_BIN_TREE, true, true, table.getSchema());
  }
}
