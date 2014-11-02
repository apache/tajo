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
import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.OverridableConf;
import org.apache.tajo.QueryUnitAttemptId;
import org.apache.tajo.TajoConstants;
import org.apache.tajo.catalog.*;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.catalog.proto.CatalogProtos.FragmentProto;
import org.apache.tajo.catalog.proto.CatalogProtos.StoreType;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.plan.logical.*;
import org.apache.tajo.plan.rewrite.RewriteRule;
import org.apache.tajo.storage.fragment.FileFragment;
import org.apache.tajo.storage.fragment.Fragment;
import org.apache.tajo.storage.fragment.FragmentConvertor;
import org.apache.tajo.storage.hbase.HBaseStorageManager;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.URI;
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
   * It moves a result data stored in a staging output dir into a final output dir.
   * @param queryContext
   * @param finalEbId
   * @param schema
   * @param tableDesc
   * @return
   * @throws IOException
   */
  public abstract Path commitOutputData(OverridableConf queryContext, ExecutionBlockId finalEbId, Schema schema,
                                        TableDesc tableDesc) throws IOException;

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
  public abstract void beforeCATS(LogicalNode node) throws IOException;

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

  public Appender getAppender(QueryUnitAttemptId taskAttemptId, TableMeta meta, Schema schema, Path workDir)
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
}
