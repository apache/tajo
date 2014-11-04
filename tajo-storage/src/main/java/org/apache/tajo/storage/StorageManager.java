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
import org.apache.tajo.plan.logical.*;
import org.apache.tajo.plan.rewrite.RewriteRule;
import org.apache.tajo.storage.fragment.FileFragment;
import org.apache.tajo.storage.fragment.Fragment;
import org.apache.tajo.storage.fragment.FragmentConvertor;
import org.apache.tajo.storage.hbase.HBaseStorageManager;
import org.apache.tajo.util.TUtil;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.URI;
import java.text.NumberFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * StorageManager
 */
public abstract class StorageManager {
  private final Log LOG = LogFactory.getLog(StorageManager.class);

  protected TajoConf conf;
  protected StoreType storeType;

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
   *
   * @throws IOException
   */
  protected abstract void storageInit() throws IOException;

  /**
   *
   * @param tableDesc
   * @param ifNotExists
   * @throws IOException
   */
  public abstract void createTable(TableDesc tableDesc, boolean ifNotExists) throws IOException;

  /**
   *
   * @param tableDesc
   * @throws IOException
   */
  public abstract void purgeTable(TableDesc tableDesc) throws IOException;

  /**
   *
   * @param fragmentId
   * @param tableDesc
   * @param scanNode
   * @return
   * @throws IOException
   */
  public abstract List<Fragment> getSplits(String fragmentId, TableDesc tableDesc,
                                           ScanNode scanNode) throws IOException;

  /**
   *
   * @param tableDesc
   * @param currentPage
   * @param numFragments
   * @return
   * @throws IOException
   */
  public abstract List<Fragment> getNonForwardSplit(TableDesc tableDesc, int currentPage, int numFragments)
      throws IOException;

  /**
   * @return
   */
  public abstract StorageProperty getStorageProperty();

  /**
   * Release storage manager resource
   */
  public abstract void closeStorageManager();

  /**
   *
   * @param queryContext
   * @param tableDesc
   * @param inputSchema
   * @param sortSpecs
   * @return
   * @throws IOException
   */
  public abstract TupleRange[] getInsertSortRanges(OverridableConf queryContext, TableDesc tableDesc,
                                                   Schema inputSchema, SortSpec[] sortSpecs,
                                                   TupleRange dataRange) throws IOException;

  /**
   * @param node
   * @throws IOException
   */
  public abstract void beforeInsertOrCATS(LogicalNode node) throws IOException;

  /**
   *
   * @param node
   * @throws IOException
   */
  public abstract void queryFailed(LogicalNode node) throws IOException;

  public StoreType getStoreType() {
    return storeType;
  }

  public void init(TajoConf tajoConf) throws IOException {
    this.conf = tajoConf;
    storageInit();
  }

  public void close() throws IOException {
    synchronized(storageManagers) {
      for (StorageManager eachStorageManager: storageManagers.values()) {
        eachStorageManager.closeStorageManager();
      }
    }
  }

  public List<Fragment> getSplits(String fragmentId, TableDesc tableDesc) throws IOException {
    return getSplits(fragmentId, tableDesc, null);
  }

  public static FileStorageManager getFileStorageManager(TajoConf tajoConf) throws IOException {
    return getFileStorageManager(tajoConf, null);
  }

  public static FileStorageManager getFileStorageManager(TajoConf tajoConf, Path warehousePath) throws IOException {
    URI uri;
    TajoConf copiedConf = new TajoConf(tajoConf);
    if (warehousePath != null) {
      copiedConf.setVar(ConfVars.WAREHOUSE_DIR, warehousePath.toUri().toString());
    }
    uri = TajoConf.getWarehouseDir(copiedConf).toUri();
    String key = "file".equals(uri.getScheme()) ? "file" : uri.toString();
    return (FileStorageManager) getStorageManager(copiedConf, StoreType.CSV, key);
  }

  public static StorageManager getStorageManager(TajoConf tajoConf, String storeType) throws IOException {
    if ("HBASE".equals(storeType)) {
      return getStorageManager(tajoConf, StoreType.HBASE);
    } else {
      return getStorageManager(tajoConf, StoreType.CSV);
    }
  }

  public static StorageManager getStorageManager(TajoConf tajoConf, StoreType storeType) throws IOException {
    return getStorageManager(tajoConf, storeType, null);
  }

  public static synchronized StorageManager getStorageManager (
      TajoConf conf, StoreType storeType, String managerKey) throws IOException {
    synchronized (storageManagers) {
      String storeKey = storeType + managerKey;
      StorageManager manager = storageManagers.get(storeKey);
      if (manager == null) {
        switch (storeType) {
          case HBASE:
            manager = new HBaseStorageManager(storeType);
            break;
          default:
            manager = new FileStorageManager(storeType);
        }

        manager.init(conf);
        storageManagers.put(storeKey, manager);
      }

      return manager;
    }
  }

  public Scanner getScanner(TableMeta meta, Schema schema, FragmentProto fragment, Schema target) throws IOException {
    return getScanner(meta, schema, FragmentConvertor.convert(conf, fragment), target);
  }

  public Scanner getScanner(TableMeta meta, Schema schema, Fragment fragment) throws IOException {
    return getScanner(meta, schema, fragment, schema);
  }

  public Appender getAppender(OverridableConf queryContext,
                              QueryUnitAttemptId taskAttemptId, TableMeta meta, Schema schema, Path workDir)
      throws IOException {
    Appender appender;

    Class<? extends Appender> appenderClass;

    String handlerName = meta.getStoreType().name().toLowerCase();
    appenderClass = APPENDER_HANDLER_CACHE.get(handlerName);
    if (appenderClass == null) {
      appenderClass = conf.getClass(
          String.format("tajo.storage.appender-handler.%s.class",
              meta.getStoreType().name().toLowerCase()), null, Appender.class);
      APPENDER_HANDLER_CACHE.put(handlerName, appenderClass);
    }

    if (appenderClass == null) {
      throw new IOException("Unknown Storage Type: " + meta.getStoreType());
    }

    appender = newAppenderInstance(appenderClass, conf, taskAttemptId, meta, schema, workDir);

    return appender;
  }

  private static final Class<?>[] DEFAULT_SCANNER_PARAMS = {
      Configuration.class,
      Schema.class,
      TableMeta.class,
      Fragment.class
  };

  private static final Class<?>[] DEFAULT_APPENDER_PARAMS = {
      Configuration.class,
      QueryUnitAttemptId.class,
      Schema.class,
      TableMeta.class,
      Path.class
  };

  /**
   * create a scanner instance.
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
   * create a scanner instance.
   */
  public static <T> T newAppenderInstance(Class<T> theClass, Configuration conf, QueryUnitAttemptId taskAttemptId,
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

  public Class<? extends Scanner> getScannerClass(CatalogProtos.StoreType storeType) throws IOException {
    String handlerName = storeType.name().toLowerCase();
    Class<? extends Scanner> scannerClass = SCANNER_HANDLER_CACHE.get(handlerName);
    if (scannerClass == null) {
      scannerClass = conf.getClass(
          String.format("tajo.storage.scanner-handler.%s.class",storeType.name().toLowerCase()), null, Scanner.class);
      SCANNER_HANDLER_CACHE.put(handlerName, scannerClass);
    }

    if (scannerClass == null) {
      throw new IOException("Unknown Storage Type: " + storeType.name());
    }

    return scannerClass;
  }

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

  public static synchronized SeekableScanner getSeekableScanner(
      TajoConf conf, TableMeta meta, Schema schema, FileFragment fragment, Schema target) throws IOException {
    return (SeekableScanner)getStorageManager(conf, meta.getStoreType()).getScanner(meta, schema, fragment, target);
  }

  public static synchronized SeekableScanner getSeekableScanner(
      TajoConf conf, TableMeta meta, Schema schema, Path path) throws IOException {

    FileSystem fs = path.getFileSystem(conf);
    FileStatus status = fs.getFileStatus(path);
    FileFragment fragment = new FileFragment(path.getName(), path, 0, status.getLen());

    return getSeekableScanner(conf, meta, schema, fragment, schema);
  }

  public static long getFragmentLength(TajoConf conf, Fragment fragment) {
    if (fragment.getLength() == TajoConstants.UNKNOWN_LENGTH) {
      return conf.getLongVar(ConfVars.FRAGMENT_ALTERNATIVE_UNKNOWN_LENGTH);
    } else {
      return fragment.getLength();
    }
  }

  public void verifyInsertTableSchema(TableDesc tableDesc, Schema outSchema) throws IOException {
    // nothing to do
  }

  public List<RewriteRule> getRewriteRules(OverridableConf queryContext, TableDesc tableDesc) throws IOException {
    return null;
  }

  public Path commitOutputData(OverridableConf queryContext, ExecutionBlockId finalEbId,
                               LogicalPlan plan, Schema schema,
                               TableDesc tableDesc) throws IOException {
    return commitOutputData(queryContext, finalEbId, plan, schema, tableDesc, true);
  }

  protected Path commitOutputData(OverridableConf queryContext, ExecutionBlockId finalEbId,
                               LogicalPlan plan, Schema schema,
                               TableDesc tableDesc, boolean changeFileSeq) throws IOException {
    Path stagingDir = new Path(queryContext.get(QueryVars.STAGING_DIR));
    Path stagingResultDir = new Path(stagingDir, TajoConstants.RESULT_DIR_NAME);
    Path finalOutputDir;
    if (!queryContext.get(QueryVars.OUTPUT_TABLE_PATH, "").isEmpty()) {
      finalOutputDir = new Path(queryContext.get(QueryVars.OUTPUT_TABLE_PATH));
      FileSystem fs = stagingResultDir.getFileSystem(conf);

      if (queryContext.getBool(QueryVars.OUTPUT_OVERWRITE, false)) { // INSERT OVERWRITE INTO

        // It moves the original table into the temporary location.
        // Then it moves the new result table into the original table location.
        // Upon failed, it recovers the original table if possible.
        boolean movedToOldTable = false;
        boolean committed = false;
        Path oldTableDir = new Path(stagingDir, TajoConstants.INSERT_OVERWIRTE_OLD_TABLE_NAME);

        if (!queryContext.get(QueryVars.OUTPUT_PARTITIONS, "").isEmpty()) {
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
        } else {
          try {
            if (fs.exists(finalOutputDir)) {
              fs.rename(finalOutputDir, oldTableDir);
              movedToOldTable = fs.exists(oldTableDir);
            } else { // if the parent does not exist, make its parent directory.
              fs.mkdirs(finalOutputDir.getParent());
            }

            fs.rename(stagingResultDir, finalOutputDir);
            committed = fs.exists(finalOutputDir);
          } catch (IOException ioe) {
            // recover the old table
            if (movedToOldTable && !committed) {
              fs.rename(oldTableDir, finalOutputDir);
            }
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
          fs.rename(stagingResultDir, finalOutputDir);
          LOG.info("Moved from the staging dir to the output directory '" + finalOutputDir);
        }
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
   * @throws IOException
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
   * @throws IOException
   */
  private String replaceFileNameSeq(Path path, int seq, NumberFormat nf) throws IOException {
    String[] tokens = path.getName().split("-");
    if (tokens.length != 4) {
      throw new IOException("Wrong result file name:" + path);
    }
    return tokens[0] + "-" + tokens[1] + "-" + tokens[2] + "-" + nf.format(seq);
  }

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
   * @throws IOException
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
