/*
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
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import net.minidev.json.JSONObject;
import net.minidev.json.parser.JSONParser;
import net.minidev.json.parser.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.TajoConstants;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.storage.fragment.Fragment;
import org.apache.tajo.util.FileUtil;

import javax.annotation.Nullable;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.URI;
import java.util.Map;
import java.util.UUID;

/**
 * It handles available table spaces and cache TableSpace instances.
 *
 * Default tablespace must be a filesystem-based one.
 * HDFS and S3 can be a default tablespace if a Tajo cluster is in fully distributed mode.
 * Local file system can be a default tablespace if a Tajo cluster runs on a single machine.
 */
public class TableSpaceManager {
  private static final Log LOG = LogFactory.getLog(TableSpaceManager.class);

  /** default tablespace name */
  public static final String DEFAULT_TABLESPACE_NAME = "default";

  private static final TajoConf systemConf = new TajoConf();
  private static final JSONParser parser = new JSONParser(JSONParser.MODE_JSON_SIMPLE | JSONParser.IGNORE_CONTROL_CHAR);
  private static JSONObject config;


  // The relation ship among name, URI, Tablespaces must be kept 1:1:1.
  protected final static Map<String, URI> SPACES_URIS_MAP = Maps.newConcurrentMap();
  protected final static Map<URI, Tablespace> TABLE_SPACES = Maps.newConcurrentMap();

  protected final static Map<Class<?>, Constructor<?>> CONSTRUCTORS = Maps.newConcurrentMap();
  protected final static Map<String, Class<? extends Tablespace>> TABLE_SPACE_HANDLERS = Maps.newConcurrentMap();

  public static final Class [] TABLESPACE_PARAM = new Class [] {String.class, URI.class};

  static {
    instance = new TableSpaceManager();
  }
    /**
   * Singleton instance
   */
  private static final TableSpaceManager instance;

  TableSpaceManager(String json) {
    loadJson(json);
    loadStorages(config);

    loadSpaces(config);
    addLocalFsTablespace();
  }

  private TableSpaceManager() {
    String json;
    try {
      json = FileUtil.readTextFileFromResource("storage-default.json");
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    loadJson(json);
    loadStorages(config);
    loadSpaces(config);

    addLocalFsTablespace();
  }

  private void addLocalFsTablespace() {
    if (!(TABLE_SPACES.containsKey("file:/") ||
        !TABLE_SPACES.containsKey("file://") ||
        !TABLE_SPACES.containsKey("file:///"))) {
      String tmpName = UUID.randomUUID().toString();
      addTableSpace(tmpName, URI.create("file:/"));
    }
  }

  /**
   * Return length of the fragment.
   * In the UNKNOWN_LENGTH case get FRAGMENT_ALTERNATIVE_UNKNOWN_LENGTH from the configuration.
   *
   * @param conf Tajo system property
   * @param fragment Fragment
   * @return
   */
  public static long guessFragmentVolume(TajoConf conf, Fragment fragment) {
    if (fragment.getLength() == TajoConstants.UNKNOWN_LENGTH) {
      return conf.getLongVar(TajoConf.ConfVars.FRAGMENT_ALTERNATIVE_UNKNOWN_LENGTH);
    } else {
      return fragment.getLength();
    }
  }

  private void loadJson(String json) {
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

  private static Tablespace newTableSpace(String spaceName, URI uri) {
    Preconditions.checkNotNull(uri.getScheme(), "URI must include scheme, but it was " + uri);
    Class<? extends Tablespace> clazz = TABLE_SPACE_HANDLERS.get(uri.getScheme());

    if (clazz == null) {
      LOG.warn("There is no tablespace for " + uri.toString());
    }

    try {
      Constructor<? extends Tablespace> constructor =
          (Constructor<? extends Tablespace>) CONSTRUCTORS.get(clazz);

      if (constructor == null) {
        constructor = clazz.getDeclaredConstructor(TABLESPACE_PARAM);
        constructor.setAccessible(true);
        CONSTRUCTORS.put(clazz, constructor);
      }

      return constructor.newInstance(new Object[]{spaceName, uri});
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void loadSpaces(JSONObject root) {
    JSONObject spaces = (JSONObject) root.get(KEY_SPACES);
    for (Map.Entry<String, Object> entry : spaces.entrySet()) {
      loadTableSpace(entry.getKey(), (JSONObject) entry.getValue());
    }
  }

  public static void loadTableSpace(String spaceName, JSONObject spaceDesc) {
    boolean defaultSpace = Boolean.parseBoolean(spaceDesc.getAsString("default"));
    URI spaceUri = URI.create(spaceDesc.getAsString("uri"));

    if (defaultSpace) {
      addTableSpace(DEFAULT_TABLESPACE_NAME, spaceUri);
    }
    addTableSpace(spaceName, spaceUri);
  }

  public static void addTableSpace(String spaceName, URI uri) {
    addTableSpace(spaceName, uri, null);
  }

  private static void addTableSpace(String spaceName, URI uri, @Nullable Map<String, String> configs) {
    Tablespace tableSpace = newTableSpace(spaceName, uri);
    try {
      tableSpace.init(systemConf);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    if (configs != null) {
      tableSpace.setConfigs(configs);
    }
    addTableSpaceInternal(tableSpace);

    // If the arbitrary path is allowed, root uri is also added as a tablespace
    if (tableSpace.getProperty().isArbitraryPathAllowed()) {
      URI rootUri = tableSpace.getRootUri();
      if (!TABLE_SPACES.containsKey(rootUri)) {
        String tmpName = UUID.randomUUID().toString();
        addTableSpace(tmpName, rootUri, configs);
      }
    }
  }

  private static void addTableSpaceInternal(Tablespace space) {
    // It is a device to keep the relationship among name, URI, and tablespace 1:1:1.

    boolean nameExist = SPACES_URIS_MAP.containsKey(space.getName());
    boolean uriExist = TABLE_SPACES.containsKey(space.uri);

    boolean mismatch = nameExist && !SPACES_URIS_MAP.get(space.getName()).equals(space.getUri());
    mismatch = mismatch || uriExist && TABLE_SPACES.get(space.uri).equals(space);

    if (mismatch) {
      throw new RuntimeException("Name or URI of Tablespace must be unique.");
    }

    SPACES_URIS_MAP.put(space.getName(), space.getUri());
    // We must guarantee that the same uri results in the same tablespace instance.
    TABLE_SPACES.put(space.getUri(), space);
  }

  @VisibleForTesting
  public static Optional<Tablespace> addTableSpaceForTest(Tablespace space) {
    Tablespace existing = null;
    synchronized (SPACES_URIS_MAP) {
      // Remove existing one
      SPACES_URIS_MAP.remove(space.getName());
      existing = TABLE_SPACES.remove(space.getUri());

      // Add anotherone for test
      addTableSpace(space.name, space.uri);
    }
    // if there is an existing one, return it.
    return Optional.fromNullable(existing);
  }

  private void loadFormats(String json) {
    JSONObject spaces = (JSONObject) config.get("formats");
    for (Map.Entry<String, Object> entry : spaces.entrySet()) {

    }
  }

  public Iterable<String> getSupportSchemes() {
    return TABLE_SPACE_HANDLERS.keySet();
  }

  public static <T extends Tablespace> Optional<T> get(String uri) {
    Tablespace lastFound = null;
    for (Map.Entry<URI, Tablespace> entry: TABLE_SPACES.entrySet()) {
      if (uri.startsWith(entry.getKey().toString())) {
        if (lastFound == null || lastFound.getUri().toString().length() < uri.toString().length()) {
          lastFound = entry.getValue();
        }
      }
    }

    if (lastFound != null) {
      return (Optional<T>) Optional.of(lastFound);
    } else {
      return Optional.absent();
    }
  }

  public static <T extends Tablespace> Optional<T> get(URI uri) {
    return get(uri.toString());
  }

  /**
   * It returns the default tablespace. This method ensures that it always return the tablespace.
   *
   * @return
   */
  public static <T extends Tablespace> T getDefault() {
    return (T) getByName(DEFAULT_TABLESPACE_NAME).get();
  }

  private static final URI LOCAL_FS = URI.create("file:/");

  public static <T extends Tablespace> T getLocalFs(String name) {
    return (T) get(LOCAL_FS).get();
  }

  public static <T extends Tablespace> Optional<T> getByName(String name) {
    URI uri = SPACES_URIS_MAP.get(name);
    if (uri != null) {
      return (Optional<T>) Optional.of(TABLE_SPACES.get(uri));
    } else {
      return Optional.absent();
    }
  }
}
