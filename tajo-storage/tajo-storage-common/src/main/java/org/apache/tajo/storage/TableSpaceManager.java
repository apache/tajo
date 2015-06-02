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
import com.google.common.collect.Maps;
import net.minidev.json.JSONObject;
import net.minidev.json.parser.JSONParser;
import net.minidev.json.parser.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.util.FileUtil;

import javax.annotation.Nullable;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.URI;
import java.util.Map;

/**
 * It handles available table spaces and cache TableSpace instances.
 */
public class TableSpaceManager {
  private static final Log LOG = LogFactory.getLog(TableSpaceManager.class);

  /** default tablespace name */
  public static final String DEFAULT_SPACE_NAME = "default";

  private static TajoConf systemConf;
  private static JSONParser parser;
  private static JSONObject config;

  /**
   * Cache of all tablespace handlers
   */
  protected static final Map<String, URI> SPACES_URIS_MAP;
  protected static final Map<URI, Tablespace> TABLE_SPACES;
  protected static final Map<Class<?>, Constructor<?>> CONSTRUCTORS;
  protected static final Map<String, Class<? extends Tablespace>> TABLE_SPACE_HANDLERS;
  public static final Class [] TABLESPACE_PARAM = new Class [] {String.class, URI.class};

  static {
    systemConf = new TajoConf();

    parser = new JSONParser(JSONParser.MODE_JSON_SIMPLE | JSONParser.IGNORE_CONTROL_CHAR);

    SPACES_URIS_MAP = Maps.newConcurrentMap();
    TABLE_SPACES = Maps.newConcurrentMap();
    CONSTRUCTORS = Maps.newConcurrentMap();
    TABLE_SPACE_HANDLERS = Maps.newConcurrentMap();

    instance = new TableSpaceManager();
  }
    /**
   * Singleton instance
   */
  private static final TableSpaceManager instance;


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
      addTableSpace(DEFAULT_SPACE_NAME, spaceUri);
    }
    addTableSpace(spaceName, spaceUri);
  }

  public static void addTableSpace(String spaceName, URI uri) {
    addTableSpace(spaceName, uri, null);
  }

  public static void addTableSpace(String spaceName, URI uri, @Nullable Map<String, String> configs) {
    Tablespace tableSpace = newTableSpace(spaceName, uri);
    try {
      tableSpace.init(systemConf);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    if (configs != null) {
      tableSpace.setConfigs(configs);
    }

    addTableSpace(tableSpace);
  }

  public static void addTableSpace(Tablespace space) {
    if (SPACES_URIS_MAP.containsKey(space.getName())) {
      if (SPACES_URIS_MAP.get(space.getName()).equals(space.getUri())) {
        return;
      } else {
        throw new RuntimeException("Name of Tablespace must be unique.");
      }
    }

    SPACES_URIS_MAP.put(space.getName(), space.getUri());
    TABLE_SPACES.put(space.getUri(), space);
  }

  @VisibleForTesting
  public static void addTableSpaceForTest(Tablespace space) {
    SPACES_URIS_MAP.put(space.getName(), space.getUri());
    TABLE_SPACES.put(space.getUri(), space);
  }

  private void loadFormats(String json) {
    JSONObject spaces = (JSONObject) config.get("formats");
    for (Map.Entry<String, Object> entry : spaces.entrySet()) {

    }
  }

  public Iterable<String> getSupportSchemes() {
    return TABLE_SPACE_HANDLERS.keySet();
  }

  public static <T extends Tablespace> Optional<T> get(URI uri) {
    for (Map.Entry<URI, Tablespace> entry: TABLE_SPACES.entrySet()) {
      if (entry.getKey().toString().startsWith(uri.toString())) {
        return (Optional<T>) Optional.of(entry.getValue());
      }
    }
    return Optional.absent();
  }

  /**
   * It returns the default tablespace. This method ensures that it always return the tablespace.
   *
   * @return
   */
  public static <T extends Tablespace> T getDefault() {
    return (T) getByName(DEFAULT_SPACE_NAME).get();
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
