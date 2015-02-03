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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.tajo.*;
import org.apache.tajo.catalog.*;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.catalog.proto.CatalogProtos.FragmentProto;
import org.apache.tajo.catalog.proto.CatalogProtos.StoreType;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.plan.LogicalPlan;
import org.apache.tajo.plan.logical.LogicalNode;
import org.apache.tajo.plan.logical.NodeType;
import org.apache.tajo.plan.logical.ScanNode;
import org.apache.tajo.plan.rewrite.LogicalPlanRewriteRule;
import org.apache.tajo.storage.fragment.Fragment;
import org.apache.tajo.storage.fragment.FragmentConvertor;
import org.apache.tajo.util.TUtil;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.text.NumberFormat;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * StorageManager manages the functions of storing and reading data.
 * StorageManager is a abstract class.
 * For supporting such as HDFS, HBASE, a specific StorageManager should be implemented by inheriting this class.
 *
 */
public abstract class StorageManager {
  private final Log LOG = LogFactory.getLog(StorageManager.class);

  private static final Class<?>[] DEFAULT_SCANNER_PARAMS = {
      Configuration.class,
      Schema.class,
      TableMeta.class,
      Fragment.class
  };

  private static final Class<?>[] DEFAULT_APPENDER_PARAMS = {
      Configuration.class,
      TaskAttemptId.class,
      Schema.class,
      TableMeta.class,
      Path.class
  };

  public static final PathFilter hiddenFileFilter = new PathFilter() {
    public boolean accept(Path p) {
      String name = p.getName();
      return !name.startsWith("_") && !name.startsWith(".");
    }
  };

  protected TajoConf conf;
  protected StoreType storeType;

  /**
   * Cache of StorageManager.
   * Key is manager key(warehouse path) + store type
   */
  private static final Map<String, StorageManager> storageManagers = Maps.newHashMap();

  /**
   * Cache of scanner handlers for each storage type.
   */
  protected static final Map<String, Class<? extends Scanner>> SCANNER_HANDLER_CACHE
      = new ConcurrentHashMap<String, Class<? extends Scanner>>();

  /**
   * Cache of appender handlers for each storage type.
   */
  protected static final Map<String, Class<? extends Appender>> APPENDER_HANDLER_CACHE
      = new ConcurrentHashMap<String, Class<? extends Appender>>();

  /**
   * Cache of constructors for each class. Pins the classes so they
   * can't be garbage collected until ReflectionUtils can be collected.
   */
  private static final Map<Class<?>, Constructor<?>> CONSTRUCTOR_CACHE =
      new ConcurrentHashMap<Class<?>, Constructor<?>>();

  public StorageManager(StoreType storeType) {
    this.storeType = storeType;
  }

  /**
   * Initialize storage manager.
   * @throws java.io.IOException
   */
  protected abstract void storageInit() throws IOException;

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
  public abstract StorageProperty getStorageProperty();

  /**
   * Release storage manager resource
   */
  public abstract void closeStorageManager();


  /**
   * Clear all class cache
   */
  @VisibleForTesting
  protected synchronized static void clearCache() {
    CONSTRUCTOR_CACHE.clear();
    SCANNER_HANDLER_CACHE.clear();
    APPENDER_HANDLER_CACHE.clear();
    storageManagers.clear();
  }

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
                                                   Schema inputSchema, SortSpec[] sortSpecs,
                                                   TupleRange dataRange) throws IOException;

  /**
   * This method is called before executing 'INSERT' or 'CREATE TABLE as SELECT'.
   * In general Tajo creates the target table after finishing the final sub-query of CATS.
   * But In the special cases, such as HBase INSERT or CAST query uses the target table information.
   * That kind of the storage should implements the logic related to creating table in this method.
   *
   * @param node The child node of the root node.
   * @throws java.io.IOException
   */
  public abstract void beforeInsertOrCATS(LogicalNode node) throws IOException;

  /**
   * It is called when the query failed.
   * Each storage manager should implement to be processed when the query fails in this method.
   *
   * @param node The child node of the root node.
   * @throws java.io.IOException
   */
  public abstract void rollbackOutputCommit(LogicalNode node) throws IOException;

  /**
   * Returns the current storage type.
   * @return
   */
  public StoreType getStoreType() {
    return storeType;
  }

  /**
   * Initialize StorageManager instance. It should be called before using.
   *
   * @param tajoConf
   * @throws java.io.IOException
   */
  public void init(TajoConf tajoConf) throws IOException {
    this.conf = tajoConf;
    storageInit();
  }

  /**
   * Close StorageManager
   * @throws java.io.IOException
   */
  public static void close() throws IOException {
    synchronized(storageManagers) {
      for (StorageManager eachStorageManager: storageManagers.values()) {
        eachStorageManager.closeStorageManager();
      }
    }
    clearCache();
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
   * Returns FileStorageManager instance.
   *
   * @param tajoConf Tajo system property.
   * @return
   * @throws java.io.IOException
   */
  public static StorageManager getFileStorageManager(TajoConf tajoConf) throws IOException {
    return getStorageManager(tajoConf, StoreType.CSV);
  }

  /**
   * Returns the proper StorageManager instance according to the storeType.
   *
   * @param tajoConf Tajo system property.
   * @param storeType Storage type
   * @return
   * @throws java.io.IOException
   */
  public static StorageManager getStorageManager(TajoConf tajoConf, String storeType) throws IOException {
    if ("HBASE".equalsIgnoreCase(storeType)) {
      return getStorageManager(tajoConf, StoreType.HBASE);
    } else {
      return getStorageManager(tajoConf, StoreType.CSV);
    }
  }

  /**
   * Returns the proper StorageManager instance according to the storeType.
   *
   * @param tajoConf Tajo system property.
   * @param storeType Storage type
   * @return
   * @throws java.io.IOException
   */
  public static StorageManager getStorageManager(TajoConf tajoConf, StoreType storeType) throws IOException {
    FileSystem fileSystem = TajoConf.getWarehouseDir(tajoConf).getFileSystem(tajoConf);
    if (fileSystem != null) {
      return getStorageManager(tajoConf, storeType, fileSystem.getUri().toString());
    } else {
      return getStorageManager(tajoConf, storeType, null);
    }
  }

  /**
   * Returns the proper StorageManager instance according to the storeType
   *
   * @param tajoConf Tajo system property.
   * @param storeType Storage type
   * @param managerKey Key that can identify each storage manager(may be a path)
   * @return
   * @throws java.io.IOException
   */
  private static synchronized StorageManager getStorageManager (
      TajoConf tajoConf, StoreType storeType, String managerKey) throws IOException {

    String typeName;
    switch (storeType) {
      case HBASE:
        typeName = "hbase";
        break;
      default:
        typeName = "hdfs";
    }

    synchronized (storageManagers) {
      String storeKey = typeName + "_" + managerKey;
      StorageManager manager = storageManagers.get(storeKey);

      if (manager == null) {
        Class<? extends StorageManager> storageManagerClass =
            tajoConf.getClass(String.format("tajo.storage.manager.%s.class", typeName), null, StorageManager.class);

        if (storageManagerClass == null) {
          throw new IOException("Unknown Storage Type: " + typeName);
        }

        try {
          Constructor<? extends StorageManager> constructor =
              (Constructor<? extends StorageManager>) CONSTRUCTOR_CACHE.get(storageManagerClass);
          if (constructor == null) {
            constructor = storageManagerClass.getDeclaredConstructor(new Class<?>[]{StoreType.class});
            constructor.setAccessible(true);
            CONSTRUCTOR_CACHE.put(storageManagerClass, constructor);
          }
          manager = constructor.newInstance(new Object[]{storeType});
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
        manager.init(tajoConf);
        storageManagers.put(storeKey, manager);
      }

      return manager;
    }
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
    scanner = newScannerInstance(scannerClass, conf, schema, meta, fragment);
    if (scanner.isProjectable()) {
      scanner.setTarget(target.toArray());
    }

    return scanner;
  }

  /**
   * Returns Scanner instance.
   *
   * @param conf The system property
   * @param meta The table meta
   * @param schema The input schema
   * @param fragment The fragment for scanning
   * @param target The output schema
   * @return Scanner instance
   * @throws java.io.IOException
   */
  public static synchronized SeekableScanner getSeekableScanner(
      TajoConf conf, TableMeta meta, Schema schema, Fragment fragment, Schema target) throws IOException {
    return (SeekableScanner)getStorageManager(conf, meta.getStoreType()).getScanner(meta, schema, fragment, target);
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

    String handlerName = CatalogUtil.getStoreTypeString(meta.getStoreType()).toLowerCase();
    appenderClass = APPENDER_HANDLER_CACHE.get(handlerName);
    if (appenderClass == null) {
      appenderClass = conf.getClass(
          String.format("tajo.storage.appender-handler.%s.class", handlerName), null, Appender.class);
      APPENDER_HANDLER_CACHE.put(handlerName, appenderClass);
    }

    if (appenderClass == null) {
      throw new IOException("Unknown Storage Type: " + meta.getStoreType());
    }

    appender = newAppenderInstance(appenderClass, conf, taskAttemptId, meta, schema, workDir);

    return appender;
  }

  /**
   * Creates a scanner instance.
   *
   * @param theClass Concrete class of scanner
   * @param conf System property
   * @param schema Input schema
   * @param meta Table meta data
   * @param fragment The fragment for scanning
   * @param <T>
   * @return The scanner instance
   */
  public static <T> T newScannerInstance(Class<T> theClass, Configuration conf, Schema schema, TableMeta meta,
                                         Fragment fragment) {
    T result;
    try {
      Constructor<T> meth = (Constructor<T>) CONSTRUCTOR_CACHE.get(theClass);
      if (meth == null) {
        meth = theClass.getDeclaredConstructor(DEFAULT_SCANNER_PARAMS);
        meth.setAccessible(true);
        CONSTRUCTOR_CACHE.put(theClass, meth);
      }
      result = meth.newInstance(new Object[]{conf, schema, meta, fragment});
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    return result;
  }

  /**
   * Creates a scanner instance.
   *
   * @param theClass Concrete class of scanner
   * @param conf System property
   * @param taskAttemptId Task id
   * @param meta Table meta data
   * @param schema Input schema
   * @param workDir Working directory
   * @param <T>
   * @return The scanner instance
   */
  public static <T> T newAppenderInstance(Class<T> theClass, Configuration conf, TaskAttemptId taskAttemptId,
                                          TableMeta meta, Schema schema, Path workDir) {
    T result;
    try {
      Constructor<T> meth = (Constructor<T>) CONSTRUCTOR_CACHE.get(theClass);
      if (meth == null) {
        meth = theClass.getDeclaredConstructor(DEFAULT_APPENDER_PARAMS);
        meth.setAccessible(true);
        CONSTRUCTOR_CACHE.put(theClass, meth);
      }
      result = meth.newInstance(new Object[]{conf, taskAttemptId, schema, meta, workDir});
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    return result;
  }

  /**
   * Return the Scanner class for the StoreType that is defined in storage-default.xml.
   *
   * @param storeType store type
   * @return The Scanner class
   * @throws java.io.IOException
   */
  public Class<? extends Scanner> getScannerClass(CatalogProtos.StoreType storeType) throws IOException {
    String handlerName = CatalogUtil.getStoreTypeString(storeType).toLowerCase();
    Class<? extends Scanner> scannerClass = SCANNER_HANDLER_CACHE.get(handlerName);
    if (scannerClass == null) {
      scannerClass = conf.getClass(
          String.format("tajo.storage.scanner-handler.%s.class", handlerName), null, Scanner.class);
      SCANNER_HANDLER_CACHE.put(handlerName, scannerClass);
    }

    if (scannerClass == null) {
      throw new IOException("Unknown Storage Type: " + storeType.name());
    }

    return scannerClass;
  }

  /**
   * Return length of the fragment.
   * In the UNKNOWN_LENGTH case get FRAGMENT_ALTERNATIVE_UNKNOWN_LENGTH from the configuration.
   *
   * @param conf Tajo system property
   * @param fragment Fragment
   * @return
   */
  public static long getFragmentLength(TajoConf conf, Fragment fragment) {
    if (fragment.getLength() == TajoConstants.UNKNOWN_LENGTH) {
      return conf.getLongVar(ConfVars.FRAGMENT_ALTERNATIVE_UNKNOWN_LENGTH);
    } else {
      return fragment.getLength();
    }
  }

  /**
   * It is called after making logical plan. Storage manager should verify the schema for inserting.
   *
   * @param tableDesc The table description of insert target.
   * @param outSchema  The output schema of select query for inserting.
   * @throws java.io.IOException
   */
  public void verifyInsertTableSchema(TableDesc tableDesc, Schema outSchema) throws IOException {
    // nothing to do
  }

  /**
   * Returns the list of storage specified rewrite rules.
   * This values are used by LogicalOptimizer.
   *
   * @param queryContext The query property
   * @param tableDesc The description of the target table.
   * @return The list of storage specified rewrite rules
   * @throws java.io.IOException
   */
  public List<LogicalPlanRewriteRule> getRewriteRules(OverridableConf queryContext, TableDesc tableDesc) throws IOException {
    return null;
  }

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
  public Path commitOutputData(OverridableConf queryContext, ExecutionBlockId finalEbId,
                               LogicalPlan plan, Schema schema,
                               TableDesc tableDesc) throws IOException {
    return commitOutputData(queryContext, finalEbId, plan, schema, tableDesc, true);
  }

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
   * @param changeFileSeq If true change result file name with max sequence.
   * @return Saved path
   * @throws java.io.IOException
   */
  protected Path commitOutputData(OverridableConf queryContext, ExecutionBlockId finalEbId,
                               LogicalPlan plan, Schema schema,
                               TableDesc tableDesc, boolean changeFileSeq) throws IOException {
    Path stagingDir = new Path(queryContext.get(QueryVars.STAGING_DIR));
    Path stagingResultDir = new Path(stagingDir, TajoConstants.RESULT_DIR_NAME);
    Path finalOutputDir;
    if (!queryContext.get(QueryVars.OUTPUT_TABLE_PATH, "").isEmpty()) {
      finalOutputDir = new Path(queryContext.get(QueryVars.OUTPUT_TABLE_PATH));
      try {
        FileSystem fs = stagingResultDir.getFileSystem(conf);

        if (queryContext.getBool(QueryVars.OUTPUT_OVERWRITE, false)) { // INSERT OVERWRITE INTO

          // It moves the original table into the temporary location.
          // Then it moves the new result table into the original table location.
          // Upon failed, it recovers the original table if possible.
          boolean movedToOldTable = false;
          boolean committed = false;
          Path oldTableDir = new Path(stagingDir, TajoConstants.INSERT_OVERWIRTE_OLD_TABLE_NAME);
          ContentSummary summary = fs.getContentSummary(stagingResultDir);

          if (!queryContext.get(QueryVars.OUTPUT_PARTITIONS, "").isEmpty() && summary.getFileCount() > 0L) {
            // This is a map for existing non-leaf directory to rename. A key is current directory and a value is
            // renaming directory.
            Map<Path, Path> renameDirs = TUtil.newHashMap();
            // This is a map for recovering existing partition directory. A key is current directory and a value is
            // temporary directory to back up.
            Map<Path, Path> recoveryDirs = TUtil.newHashMap();

            try {
              if (!fs.exists(finalOutputDir)) {
                fs.mkdirs(finalOutputDir);
              }

              visitPartitionedDirectory(fs, stagingResultDir, finalOutputDir, stagingResultDir.toString(),
                  renameDirs, oldTableDir);

              // Rename target partition directories
              for(Map.Entry<Path, Path> entry : renameDirs.entrySet()) {
                // Backup existing data files for recovering
                if (fs.exists(entry.getValue())) {
                  String recoveryPathString = entry.getValue().toString().replaceAll(finalOutputDir.toString(),
                      oldTableDir.toString());
                  Path recoveryPath = new Path(recoveryPathString);
                  fs.rename(entry.getValue(), recoveryPath);
                  fs.exists(recoveryPath);
                  recoveryDirs.put(entry.getValue(), recoveryPath);
                }
                // Delete existing directory
                fs.delete(entry.getValue(), true);
                // Rename staging directory to final output directory
                fs.rename(entry.getKey(), entry.getValue());
              }

            } catch (IOException ioe) {
              // Remove created dirs
              for(Map.Entry<Path, Path> entry : renameDirs.entrySet()) {
                fs.delete(entry.getValue(), true);
              }

              // Recovery renamed dirs
              for(Map.Entry<Path, Path> entry : recoveryDirs.entrySet()) {
                fs.delete(entry.getValue(), true);
                fs.rename(entry.getValue(), entry.getKey());
              }

              throw new IOException(ioe.getMessage());
            }
          } else { // no partition
            try {

              // if the final output dir exists, move all contents to the temporary table dir.
              // Otherwise, just make the final output dir. As a result, the final output dir will be empty.
              if (fs.exists(finalOutputDir)) {
                fs.mkdirs(oldTableDir);

                for (FileStatus status : fs.listStatus(finalOutputDir, StorageManager.hiddenFileFilter)) {
                  fs.rename(status.getPath(), oldTableDir);
                }

                movedToOldTable = fs.exists(oldTableDir);
              } else { // if the parent does not exist, make its parent directory.
                fs.mkdirs(finalOutputDir);
              }

              // Move the results to the final output dir.
              for (FileStatus status : fs.listStatus(stagingResultDir)) {
                fs.rename(status.getPath(), finalOutputDir);
              }

              // Check the final output dir
              committed = fs.exists(finalOutputDir);

            } catch (IOException ioe) {
              // recover the old table
              if (movedToOldTable && !committed) {

                // if commit is failed, recover the old data
                for (FileStatus status : fs.listStatus(finalOutputDir, StorageManager.hiddenFileFilter)) {
                  fs.delete(status.getPath(), true);
                }

                for (FileStatus status : fs.listStatus(oldTableDir)) {
                  fs.rename(status.getPath(), finalOutputDir);
                }
              }

              throw new IOException(ioe.getMessage());
            }
          }
        } else {
          String queryType = queryContext.get(QueryVars.COMMAND_TYPE);

          if (queryType != null && queryType.equals(NodeType.INSERT.name())) { // INSERT INTO an existing table

            NumberFormat fmt = NumberFormat.getInstance();
            fmt.setGroupingUsed(false);
            fmt.setMinimumIntegerDigits(3);

            if (!queryContext.get(QueryVars.OUTPUT_PARTITIONS, "").isEmpty()) {
              for(FileStatus eachFile: fs.listStatus(stagingResultDir)) {
                if (eachFile.isFile()) {
                  LOG.warn("Partition table can't have file in a staging dir: " + eachFile.getPath());
                  continue;
                }
                moveResultFromStageToFinal(fs, stagingResultDir, eachFile, finalOutputDir, fmt, -1, changeFileSeq);
              }
            } else {
              int maxSeq = StorageUtil.getMaxFileSequence(fs, finalOutputDir, false) + 1;
              for(FileStatus eachFile: fs.listStatus(stagingResultDir)) {
                if (eachFile.getPath().getName().startsWith("_")) {
                  continue;
                }
                moveResultFromStageToFinal(fs, stagingResultDir, eachFile, finalOutputDir, fmt, maxSeq++, changeFileSeq);
              }
            }
            // checking all file moved and remove empty dir
            verifyAllFileMoved(fs, stagingResultDir);
            FileStatus[] files = fs.listStatus(stagingResultDir);
            if (files != null && files.length != 0) {
              for (FileStatus eachFile: files) {
                LOG.error("There are some unmoved files in staging dir:" + eachFile.getPath());
              }
            }
          } else { // CREATE TABLE AS SELECT (CTAS)
            if (fs.exists(finalOutputDir)) {
              for (FileStatus status : fs.listStatus(stagingResultDir)) {
                fs.rename(status.getPath(), finalOutputDir);
              }
            } else {
              fs.rename(stagingResultDir, finalOutputDir);
            }
            LOG.info("Moved from the staging dir to the output directory '" + finalOutputDir);
          }
        }

        // remove the staging directory if the final output dir is given.
        Path stagingDirRoot = stagingDir.getParent();
        fs.delete(stagingDirRoot, true);
      } catch (Throwable t) {
        LOG.error(t);
        throw new IOException(t);
      }
    } else {
      finalOutputDir = new Path(stagingDir, TajoConstants.RESULT_DIR_NAME);
    }

    return finalOutputDir;
  }

  /**
   * Attach the sequence number to the output file name and than move the file into the final result path.
   *
   * @param fs FileSystem
   * @param stagingResultDir The staging result dir
   * @param fileStatus The file status
   * @param finalOutputPath Final output path
   * @param nf Number format
   * @param fileSeq The sequence number
   * @throws java.io.IOException
   */
  private void moveResultFromStageToFinal(FileSystem fs, Path stagingResultDir,
                                          FileStatus fileStatus, Path finalOutputPath,
                                          NumberFormat nf,
                                          int fileSeq, boolean changeFileSeq) throws IOException {
    if (fileStatus.isDirectory()) {
      String subPath = extractSubPath(stagingResultDir, fileStatus.getPath());
      if (subPath != null) {
        Path finalSubPath = new Path(finalOutputPath, subPath);
        if (!fs.exists(finalSubPath)) {
          fs.mkdirs(finalSubPath);
        }
        int maxSeq = StorageUtil.getMaxFileSequence(fs, finalSubPath, false);
        for (FileStatus eachFile : fs.listStatus(fileStatus.getPath())) {
          if (eachFile.getPath().getName().startsWith("_")) {
            continue;
          }
          moveResultFromStageToFinal(fs, stagingResultDir, eachFile, finalOutputPath, nf, ++maxSeq, changeFileSeq);
        }
      } else {
        throw new IOException("Wrong staging dir:" + stagingResultDir + "," + fileStatus.getPath());
      }
    } else {
      String subPath = extractSubPath(stagingResultDir, fileStatus.getPath());
      if (subPath != null) {
        Path finalSubPath = new Path(finalOutputPath, subPath);
        if (changeFileSeq) {
          finalSubPath = new Path(finalSubPath.getParent(), replaceFileNameSeq(finalSubPath, fileSeq, nf));
        }
        if (!fs.exists(finalSubPath.getParent())) {
          fs.mkdirs(finalSubPath.getParent());
        }
        if (fs.exists(finalSubPath)) {
          throw new IOException("Already exists data file:" + finalSubPath);
        }
        boolean success = fs.rename(fileStatus.getPath(), finalSubPath);
        if (success) {
          LOG.info("Moving staging file[" + fileStatus.getPath() + "] + " +
              "to final output[" + finalSubPath + "]");
        } else {
          LOG.error("Can't move staging file[" + fileStatus.getPath() + "] + " +
              "to final output[" + finalSubPath + "]");
        }
      }
    }
  }

  /**
   * Removes the path of the parent.
   * @param parentPath
   * @param childPath
   * @return
   */
  private String extractSubPath(Path parentPath, Path childPath) {
    String parentPathStr = parentPath.toUri().getPath();
    String childPathStr = childPath.toUri().getPath();

    if (parentPathStr.length() > childPathStr.length()) {
      return null;
    }

    int index = childPathStr.indexOf(parentPathStr);
    if (index != 0) {
      return null;
    }

    return childPathStr.substring(parentPathStr.length() + 1);
  }

  /**
   * Attach the sequence number to a path.
   *
   * @param path Path
   * @param seq sequence number
   * @param nf Number format
   * @return New path attached with sequence number
   * @throws java.io.IOException
   */
  private String replaceFileNameSeq(Path path, int seq, NumberFormat nf) throws IOException {
    String[] tokens = path.getName().split("-");
    if (tokens.length != 4) {
      throw new IOException("Wrong result file name:" + path);
    }
    return tokens[0] + "-" + tokens[1] + "-" + tokens[2] + "-" + nf.format(seq);
  }

  /**
   * Make sure all files are moved.
   * @param fs FileSystem
   * @param stagingPath The stagind directory
   * @return
   * @throws java.io.IOException
   */
  private boolean verifyAllFileMoved(FileSystem fs, Path stagingPath) throws IOException {
    FileStatus[] files = fs.listStatus(stagingPath);
    if (files != null && files.length != 0) {
      for (FileStatus eachFile: files) {
        if (eachFile.isFile()) {
          LOG.error("There are some unmoved files in staging dir:" + eachFile.getPath());
          return false;
        } else {
          if (verifyAllFileMoved(fs, eachFile.getPath())) {
            fs.delete(eachFile.getPath(), false);
          } else {
            return false;
          }
        }
      }
    }

    return true;
  }

  /**
   * This method sets a rename map which includes renamed staging directory to final output directory recursively.
   * If there exists some data files, this delete it for duplicate data.
   *
   *
   * @param fs
   * @param stagingPath
   * @param outputPath
   * @param stagingParentPathString
   * @throws java.io.IOException
   */
  private void visitPartitionedDirectory(FileSystem fs, Path stagingPath, Path outputPath,
                                         String stagingParentPathString,
                                         Map<Path, Path> renameDirs, Path oldTableDir) throws IOException {
    FileStatus[] files = fs.listStatus(stagingPath);

    for(FileStatus eachFile : files) {
      if (eachFile.isDirectory()) {
        Path oldPath = eachFile.getPath();

        // Make recover directory.
        String recoverPathString = oldPath.toString().replaceAll(stagingParentPathString,
            oldTableDir.toString());
        Path recoveryPath = new Path(recoverPathString);
        if (!fs.exists(recoveryPath)) {
          fs.mkdirs(recoveryPath);
        }

        visitPartitionedDirectory(fs, eachFile.getPath(), outputPath, stagingParentPathString,
            renameDirs, oldTableDir);
        // Find last order partition for renaming
        String newPathString = oldPath.toString().replaceAll(stagingParentPathString,
            outputPath.toString());
        Path newPath = new Path(newPathString);
        if (!isLeafDirectory(fs, eachFile.getPath())) {
          renameDirs.put(eachFile.getPath(), newPath);
        } else {
          if (!fs.exists(newPath)) {
            fs.mkdirs(newPath);
          }
        }
      }
    }
  }

  private boolean isLeafDirectory(FileSystem fs, Path path) throws IOException {
    boolean retValue = false;

    FileStatus[] files = fs.listStatus(path);
    for (FileStatus file : files) {
      if (fs.isDirectory(file.getPath())) {
        retValue = true;
        break;
      }
    }

    return retValue;
  }
}
