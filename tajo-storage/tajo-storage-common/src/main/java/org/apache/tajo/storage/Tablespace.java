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

package org.apache.tajo.storage;

import com.google.common.base.Optional;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.OverridableConf;
import org.apache.tajo.QueryVars;
import org.apache.tajo.TaskAttemptId;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SortSpec;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.proto.CatalogProtos.FragmentProto;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.exception.UnsupportedException;
import org.apache.tajo.plan.LogicalPlan;
import org.apache.tajo.plan.PlanningException;
import org.apache.tajo.plan.logical.LogicalNode;
import org.apache.tajo.plan.logical.ScanNode;
import org.apache.tajo.storage.fragment.Fragment;
import org.apache.tajo.storage.fragment.FragmentConvertor;

import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Tablespace manages the functions of storing and reading data.
 * Tablespace is a abstract class.
 * For supporting such as HDFS, HBASE, a specific Tablespace should be implemented by inheriting this class.
 *
 */
public abstract class Tablespace {

  protected final String name;
  protected final URI uri;
  /** this space is visible or not. */
  protected boolean visible = true;

  protected TajoConf conf;

  public Tablespace(String name, URI uri) {
    this.name = name;
    this.uri = uri;
  }

  public void setVisible(boolean visible) {
    this.visible = visible;
  }

  public Set<String> getDependencies() {
    return Collections.emptySet();
  }

  /**
   * Initialize storage manager.
   * @throws java.io.IOException
   */
  protected abstract void storageInit() throws IOException;

  public String getName() {
    return name;
  }

  public URI getUri() {
    return uri;
  }

  public boolean isVisible() {
    return visible;
  }

  public abstract void setConfig(String name, String value);

  public abstract void setConfigs(Map<String, String> configs);

  public String toString() {
    return name + "=" + uri.toString();
  }

  public abstract long getTableVolume(URI uri) throws IOException;

  /**
   * if {@link StorageProperty#isArbitraryPathAllowed} is true,
   * the storage allows arbitrary path accesses. In this case, the storage must provide the root URI.
   *
   * @see {@link StorageProperty#isArbitraryPathAllowed}
   * @return Root URI
   */
  public URI getRootUri() {
    throw new UnsupportedException(
        String.format("Tablespace '%s' does not allow the use of artibrary paths", uri.toString()));
  }

  /**
   * Get Table URI
   *
   * @param tableName
   * @return
   */
  public abstract URI getTableUri(String databaseName, String tableName);

  /**
   * Returns the splits that will serve as input for the scan tasks. The
   * number of splits matches the number of regions in a table.
   * @param fragmentId The table name or previous ExecutionBlockId
   * @param tableDesc The table description for the target data.
   * @param scanNode The logical node for scanning.
   * @return The list of input fragments.
   * @throws java.io.IOException
   */
  public abstract List<Fragment> getSplits(String fragmentId, TableDesc tableDesc,
                                           ScanNode scanNode) throws IOException;

  /**
   * It returns the splits that will serve as input for the non-forward query scanner such as 'select * from table1'.
   * The result list should be small. If there is many fragments for scanning, TajoMaster uses the paging navigation.
   * @param tableDesc The table description for the target data.
   * @param currentPage The current page number within the entire list.
   * @param numFragments The number of fragments in the result.
   * @return The list of input fragments.
   * @throws java.io.IOException
   */
  public abstract List<Fragment> getNonForwardSplit(TableDesc tableDesc, int currentPage, int numFragments)
      throws IOException;

  /**
   * It returns the storage property.
   * @return The storage property
   */
  public abstract StorageProperty getProperty();

  public abstract FormatProperty getFormatProperty(TableMeta meta);

  /**
   * Release storage manager resource
   */
  public abstract void close();


  /**
   * It is called by a Repartitioner for range shuffling when the SortRangeType of SortNode is USING_STORAGE_MANAGER.
   * In general Repartitioner determines the partition range using previous output statistics data.
   * In the special cases, such as HBase Repartitioner uses the result of this method.
   *
   * @param queryContext The current query context which contains query properties.
   * @param tableDesc The table description for the target data.
   * @param inputSchema The input schema
   * @param sortSpecs The sort specification that contains the sort column and sort order.
   * @return The list of sort ranges.
   * @throws java.io.IOException
   */
  public abstract TupleRange[] getInsertSortRanges(OverridableConf queryContext, TableDesc tableDesc,
                                                   Schema inputSchema, SortSpec [] sortSpecs,
                                                   TupleRange dataRange) throws IOException;

  /**
   * It is called when the query failed.
   * Each storage manager should implement to be processed when the query fails in this method.
   *
   * @param node The child node of the root node.
   * @throws java.io.IOException
   */

  /**
   * Initialize Tablespace instance. It should be called before using.
   *
   * @param tajoConf
   * @throws java.io.IOException
   */
  public void init(TajoConf tajoConf) throws IOException {
    this.conf = new TajoConf(tajoConf);
    storageInit();
  }

  /**
   * Returns the splits that will serve as input for the scan tasks. The
   * number of splits matches the number of regions in a table.
   *
   * @param fragmentId The table name or previous ExecutionBlockId
   * @param tableDesc The table description for the target data.
   * @return The list of input fragments.
   * @throws java.io.IOException
   */
  public List<Fragment> getSplits(String fragmentId, TableDesc tableDesc) throws IOException {
    return getSplits(fragmentId, tableDesc, null);
  }

  /**
   * Returns Scanner instance.
   *
   * @param meta The table meta
   * @param schema The input schema
   * @param fragment The fragment for scanning
   * @param target Columns which are selected.
   * @return Scanner instance
   * @throws java.io.IOException
   */
  public Scanner getScanner(TableMeta meta, Schema schema, FragmentProto fragment, Schema target) throws IOException {
    return getScanner(meta, schema, FragmentConvertor.convert(conf, fragment), target);
  }

  /**
   * Returns Scanner instance.
   *
   * @param meta The table meta
   * @param schema The input schema
   * @param fragment The fragment for scanning
   * @return Scanner instance
   * @throws java.io.IOException
   */
  public Scanner getScanner(TableMeta meta, Schema schema, Fragment fragment) throws IOException {
    return getScanner(meta, schema, fragment, schema);
  }

  /**
   * Returns Scanner instance.
   *
   * @param meta The table meta
   * @param schema The input schema
   * @param fragment The fragment for scanning
   * @param target The output schema
   * @return Scanner instance
   * @throws java.io.IOException
   */
  public Scanner getScanner(TableMeta meta, Schema schema, Fragment fragment, Schema target) throws IOException {
    if (fragment.isEmpty()) {
      Scanner scanner = new NullScanner(conf, schema, meta, fragment);
      scanner.setTarget(target.toArray());

      return scanner;
    }

    Scanner scanner;

    Class<? extends Scanner> scannerClass = getScannerClass(meta.getStoreType());
    scanner = OldStorageManager.newScannerInstance(scannerClass, conf, schema, meta, fragment);
    scanner.setTarget(target.toArray());

    return scanner;
  }

  public Appender getAppenderForInsertRow(OverridableConf queryContext,
                                          TaskAttemptId taskAttemptId,
                                          TableMeta meta,
                                          Schema schema,
                                          Path workDir) throws IOException {
    return getAppender(queryContext, taskAttemptId, meta, schema, workDir);
  }

  /**
   * Returns Appender instance.
   * @param queryContext Query property.
   * @param taskAttemptId Task id.
   * @param meta Table meta data.
   * @param schema Output schema.
   * @param workDir Working directory
   * @return Appender instance
   * @throws java.io.IOException
   */
  public Appender getAppender(OverridableConf queryContext,
                              TaskAttemptId taskAttemptId, TableMeta meta, Schema schema, Path workDir)
      throws IOException {
    Appender appender;

    Class<? extends Appender> appenderClass;

    String handlerName = meta.getStoreType().toLowerCase();
    appenderClass = OldStorageManager.APPENDER_HANDLER_CACHE.get(handlerName);
    if (appenderClass == null) {
      appenderClass = conf.getClass(
          String.format("tajo.storage.appender-handler.%s.class", handlerName), null, Appender.class);
      OldStorageManager.APPENDER_HANDLER_CACHE.put(handlerName, appenderClass);
    }

    if (appenderClass == null) {
      throw new IOException("Unknown Storage Type: " + meta.getStoreType());
    }

    appender = OldStorageManager.newAppenderInstance(appenderClass, conf, taskAttemptId, meta, schema, workDir);

    return appender;
  }

  /**
   * Return the Scanner class for the StoreType that is defined in storage-default.xml.
   *
   * @param storeType store type
   * @return The Scanner class
   * @throws java.io.IOException
   */
  public Class<? extends Scanner> getScannerClass(String storeType) throws IOException {
    String handlerName = storeType.toLowerCase();
    Class<? extends Scanner> scannerClass = OldStorageManager.SCANNER_HANDLER_CACHE.get(handlerName);
    if (scannerClass == null) {
      scannerClass = conf.getClass(
          String.format("tajo.storage.scanner-handler.%s.class", handlerName), null, Scanner.class);
      OldStorageManager.SCANNER_HANDLER_CACHE.put(handlerName, scannerClass);
    }

    if (scannerClass == null) {
      throw new IOException("Unknown Storage Type: " + storeType);
    }

    return scannerClass;
  }

  /**
   * It is called after making logical plan. Storage manager should verify the schema for inserting.
   *
   * @param tableDesc The table description of insert target.
   * @param outSchema  The output schema of select query for inserting.
   * @throws java.io.IOException
   */
  public abstract void verifySchemaToWrite(TableDesc tableDesc, Schema outSchema) throws IOException;

  /**
   * Rewrite the logical plan. It is assumed that the final plan will be given in this method.
   */
  public void rewritePlan(OverridableConf context, LogicalPlan plan) throws PlanningException {
    // nothing to do by default
  }

  ////////////////////////////////////////////////////////////////////////////
  // Table Lifecycle Section
  ////////////////////////////////////////////////////////////////////////////

  /**
   * This method is called after executing "CREATE TABLE" statement.
   * If a storage is a file based storage, a storage manager may create directory.
   *
   * @param tableDesc Table description which is created.
   * @param ifNotExists Creates the table only when the table does not exist.
   * @throws java.io.IOException
   */
  public abstract void createTable(TableDesc tableDesc, boolean ifNotExists) throws IOException;

  /**
   * This method is called after executing "DROP TABLE" statement with the 'PURGE' option
   * which is the option to delete all the data.
   *
   * @param tableDesc
   * @throws java.io.IOException
   */
  public abstract void purgeTable(TableDesc tableDesc) throws IOException;

  /**
   * This method is called before executing 'INSERT' or 'CREATE TABLE as SELECT'.
   * In general Tajo creates the target table after finishing the final sub-query of CATS.
   * But In the special cases, such as HBase INSERT or CAST query uses the target table information.
   * That kind of the storage should implements the logic related to creating table in this method.
   *
   * @param node The child node of the root node.
   * @throws java.io.IOException
   */
  public abstract void prepareTable(LogicalNode node) throws IOException;

  /**
   * Finalizes result data. Tajo stores result data in the staging directory.
   * If the query fails, clean up the staging directory.
   * Otherwise the query is successful, move to the final directory from the staging directory.
   *
   * @param queryContext The query property
   * @param finalEbId The final execution block id
   * @param plan The query plan
   * @param schema The final output schema
   * @param tableDesc The description of the target table
   * @return Saved path
   * @throws java.io.IOException
   */
  public abstract Path commitTable(OverridableConf queryContext,
                                   ExecutionBlockId finalEbId,
                                   LogicalPlan plan, Schema schema,
                                   TableDesc tableDesc) throws IOException;

  public abstract void rollbackTable(LogicalNode node) throws IOException;

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof Tablespace) {
      Tablespace other = (Tablespace) obj;
      return name.equals(other.name) && uri.equals(other.uri);
    } else {
      return false;
    }
  }

  public abstract URI getStagingUri(OverridableConf context, String queryId, TableMeta meta) throws IOException;

  public URI prepareStagingSpace(TajoConf conf, String queryId, OverridableConf context,
                                 TableMeta meta) throws IOException {
    throw new IOException("Staging the output result is not supported in this storage");
  }
}
