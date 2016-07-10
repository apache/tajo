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
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.TaskAttemptId;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.storage.fragment.Fragment;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * It handles available table spaces and cache TableSpace instances.
 */
public class OldStorageManager {
  private static final Log LOG = LogFactory.getLog(OldStorageManager.class);

  /**
   * Cache of scanner handlers for each storage type.
   */
  protected static final Map<String, Class<? extends Scanner>> SCANNER_HANDLER_CACHE
      = new ConcurrentHashMap<>();
  /**
   * Cache of appender handlers for each storage type.
   */
  protected static final Map<String, Class<? extends Appender>> APPENDER_HANDLER_CACHE
      = new ConcurrentHashMap<>();
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
  /**
   * Cache of Tablespace.
   * Key is manager key(warehouse path) + store type
   */
  private static final Map<String, Tablespace> storageManagers = Maps.newHashMap();
  /**
   * Cache of constructors for each class. Pins the classes so they
   * can't be garbage collected until ReflectionUtils can be collected.
   */
  protected static Map<Class<?>, Constructor<?>> CONSTRUCTOR_CACHE = Maps.newConcurrentMap();

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
   * Close Tablespace
   * @throws java.io.IOException
   */
  public static void shutdown() throws IOException {
    synchronized(storageManagers) {
      storageManagers.values().forEach(Tablespace::close);
    }
    clearCache();
  }

  /**
   * Returns the proper Tablespace instance according to the dataFormat.
   *
   * @param tajoConf Tajo system property.
   * @param dataFormat Storage type
   * @return
   * @throws IOException
   */
  public static Tablespace getStorageManager(TajoConf tajoConf, String dataFormat) throws IOException {
    FileSystem fileSystem = TajoConf.getWarehouseDir(tajoConf).getFileSystem(tajoConf);
    if (fileSystem != null) {
      return getStorageManager(tajoConf, fileSystem.getUri(), dataFormat);
    } else {
      return getStorageManager(tajoConf, null, dataFormat);
    }
  }

  /**
   * Returns the proper Tablespace instance according to the dataFormat
   *
   * @param tajoConf Tajo system property.
   * @param uri Key that can identify each storage manager(may be a path)
   * @param dataFormat Storage type
   * @return
   * @throws IOException
   */
  public static synchronized Tablespace getStorageManager(
      TajoConf tajoConf, URI uri, String dataFormat) throws IOException {
    Preconditions.checkNotNull(tajoConf);
    Preconditions.checkNotNull(uri);
    Preconditions.checkNotNull(dataFormat);

    String typeName;
    if (dataFormat.equalsIgnoreCase("HBASE")) {
      typeName = "hbase";
    } else {
      typeName = "hdfs";
    }

    synchronized (storageManagers) {
      String storeKey = typeName + "_" + uri.toString();
      Tablespace manager = storageManagers.get(storeKey);

      if (manager == null) {
        Class<? extends Tablespace> storageManagerClass =
            tajoConf.getClass(String.format("tajo.storage.manager.%s.class", typeName), null, Tablespace.class);

        if (storageManagerClass == null) {
          throw new IOException("Unknown Storage Type: " + typeName);
        }

        try {
          Constructor<? extends Tablespace> constructor =
              (Constructor<? extends Tablespace>) CONSTRUCTOR_CACHE.get(storageManagerClass);
          if (constructor == null) {
            constructor = storageManagerClass.getDeclaredConstructor(TablespaceManager.TABLESPACE_PARAM);
            constructor.setAccessible(true);
            CONSTRUCTOR_CACHE.put(storageManagerClass, constructor);
          }
          manager = constructor.newInstance(new Object[]{"noname", uri, null});
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
}
