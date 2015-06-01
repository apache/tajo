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
import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import net.minidev.json.JSONObject;
import net.minidev.json.parser.JSONParser;
import net.minidev.json.parser.ParseException;
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
import org.apache.tajo.util.FileUtil;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * It handles available table spaces and cache TableSpace instances.
 */
public class TableSpaceManager {
  private static final Log LOG = LogFactory.getLog(TableSpaceManager.class);

  private TajoConf systemConf;
  private JSONParser parser;
  private JSONObject config;

  static {
    //instance = new TableSpaceManager();
  }
    /**
   * Singleton instance
   */
  //private static final TableSpaceManager instance;
  /**
   * Cache of all tablespace handlers
   */
  protected final Map<String, URI> SPACES_URIS_MAP = Maps.newConcurrentMap();
  protected final Map<URI, Tablespace> TABLE_SPACES = Maps.newConcurrentMap();
  protected final Map<String, Class<? extends Tablespace>> TABLE_SPACE_HANDLERS = Maps.newConcurrentMap();

  TableSpaceManager(String json) {
    systemConf = new TajoConf();

    loadJson(json);
    loadStorages(config);
  }

  private TableSpaceManager() {
    systemConf = new TajoConf();

    String json = null;
    try {
      json = FileUtil.readTextFileFromResource("storage-default.json");
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    loadJson(json);
    loadStorages(config);
    loadSpaces(config);
  }

  private void loadJson(String json) {
    parser = new JSONParser(JSONParser.MODE_JSON_SIMPLE | JSONParser.IGNORE_CONTROL_CHAR);

    try {
      config = (JSONObject) parser.parse(json);
    } catch (ParseException e) {
      throw new RuntimeException(e);
    }
  }

  public static final String KEY_STORAGES = "storages"; // storages
  public static final String KEY_STORAGE_HANDLER = "handler"; // storages/?/handler
  public static final String KEY_STORAGE_DEFAULT_FORMAT = "default-format"; // storages/?/default-format

  public static final String KEY_SPACES = "spaces";

  private void loadStorages(JSONObject root) {
    JSONObject spaces = (JSONObject) root.get(KEY_STORAGES);
    for (Map.Entry<String, Object> entry : spaces.entrySet()) {
      String storageType = entry.getKey();
      JSONObject storageDesc = (JSONObject) entry.getValue();
      String handlerClass = (String) storageDesc.get(KEY_STORAGE_HANDLER);
      String defaultFormat = (String) storageDesc.get(KEY_STORAGE_DEFAULT_FORMAT);

      Class<? extends Tablespace> clazz = null;
      try {
        clazz = (Class<? extends Tablespace>) Class.forName(handlerClass);
      } catch (ClassNotFoundException e) {
        LOG.warn(handlerClass + " Not Found. This handler is ignored.");
        continue;
      }

      TABLE_SPACE_HANDLERS.put(storageType, clazz);
    }
  }

  private void loadSpaces(JSONObject root) {
    CONSTRUCTOR_CACHE = Maps.newConcurrentMap();

    JSONObject spaces = (JSONObject) root.get(KEY_SPACES);
    for (Map.Entry<String, Object> entry : spaces.entrySet()) {

      String spaceName = entry.getKey();
      JSONObject spaceJson = (JSONObject) entry.getValue();
      URI spaceUri = URI.create(spaceJson.getAsString("uri"));

      Tablespace tableSpace = null;

      Class<? extends Tablespace> clazz = TABLE_SPACE_HANDLERS.get(spaceUri.getScheme());

      if (clazz == null) {
        throw new RuntimeException("There is no tablespace for " + spaceUri);
      }

      try {
        Constructor<? extends Tablespace> constructor =
            (Constructor<? extends Tablespace>) CONSTRUCTOR_CACHE.get(clazz);
        if (constructor == null) {
          constructor = clazz.getDeclaredConstructor(new Class<?>[]{});
          constructor.setAccessible(true);
          CONSTRUCTOR_CACHE.put(clazz, constructor);
        }
        tableSpace = constructor.newInstance(new Object[]{});
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      try {
        tableSpace.init(systemConf);
      } catch (IOException e) {
        e.printStackTrace();
      }

      SPACES_URIS_MAP.put(spaceName, spaceUri);
      TABLE_SPACES.put(spaceUri, tableSpace);
    }
  }

  private void loadFormats(String json) {
    JSONObject spaces = (JSONObject) config.get("formats");
    for (Map.Entry<String, Object> entry : spaces.entrySet()) {

    }
  }

  public Iterable<String> getSupportSchemes() {
    return TABLE_SPACE_HANDLERS.keySet();
  }

  public static Optional<Tablespace> get() {
    return Optional.absent();
  }

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
      for (Tablespace eachTablespace : storageManagers.values()) {
        eachTablespace.close();
      }
    }
    clearCache();
  }

  /**
   * Returns FileStorageManager instance.
   *
   * @param tajoConf Tajo system property.
   * @return
   * @throws IOException
   */
  public static Tablespace getFileStorageManager(TajoConf tajoConf) throws IOException {
    return getStorageManager(tajoConf, "CSV");
  }

  /**
   * Returns the proper Tablespace instance according to the storeType.
   *
   * @param tajoConf Tajo system property.
   * @param storeType Storage type
   * @return
   * @throws IOException
   */
  public static Tablespace getStorageManager(TajoConf tajoConf, String storeType) throws IOException {
    FileSystem fileSystem = TajoConf.getWarehouseDir(tajoConf).getFileSystem(tajoConf);
    if (fileSystem != null) {
      return getStorageManager(tajoConf, fileSystem.getUri(), storeType);
    } else {
      return getStorageManager(tajoConf, null, storeType);
    }
  }

  /**
   * Returns the proper Tablespace instance according to the storeType
   *
   * @param tajoConf Tajo system property.
   * @param uri Key that can identify each storage manager(may be a path)
   * @param storeType Storage type
   * @return
   * @throws IOException
   */
  public static synchronized Tablespace getStorageManager(
      TajoConf tajoConf, URI uri, String storeType) throws IOException {

    String typeName;
    if (storeType.equalsIgnoreCase("HBASE")) {
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
            constructor = storageManagerClass.getDeclaredConstructor(new Class<?>[]{});
            constructor.setAccessible(true);
            CONSTRUCTOR_CACHE.put(storageManagerClass, constructor);
          }
          manager = constructor.newInstance(new Object[]{});
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
   * @param conf The system property
   * @param meta The table meta
   * @param schema The input schema
   * @param fragment The fragment for scanning
   * @param target The output schema
   * @return Scanner instance
   * @throws IOException
   */
  public static synchronized SeekableScanner getSeekableScanner(
      TajoConf conf, TableMeta meta, Schema schema, Fragment fragment, Schema target) throws IOException {
    return (SeekableScanner)getStorageManager(conf, meta.getStoreType()).getScanner(meta, schema, fragment, target);
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
