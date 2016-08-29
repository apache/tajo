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
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.Maps;
import net.minidev.json.JSONObject;
import net.minidev.json.parser.JSONParser;
import net.minidev.json.parser.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.TajoConstants;
import org.apache.tajo.catalog.MetadataProvider;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.exception.TajoRuntimeException;
import org.apache.tajo.exception.UndefinedTablespaceException;
import org.apache.tajo.exception.UndefinedTablespaceHandlerException;
import org.apache.tajo.exception.UnsupportedException;
import org.apache.tajo.plan.StorageService;
import org.apache.tajo.plan.expr.EvalNode;
import org.apache.tajo.storage.fragment.Fragment;
import org.apache.tajo.util.JavaResourceUtil;
import org.apache.tajo.util.Pair;
import org.apache.tajo.util.UriUtil;

import javax.annotation.Nullable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.URI;
import java.util.*;

import static org.apache.tajo.storage.StorageConstants.LOCAL_FS_URI;

/**
 * It handles available table spaces and cache TableSpace instances.
 *
 * Default tablespace must be a filesystem-based one.
 * HDFS and S3 can be a default tablespace if a Tajo cluster is in fully distributed mode.
 * Local file system can be a default tablespace if a Tajo cluster runs on a single machine.
 */
public class TablespaceManager implements StorageService {
  private static final Log LOG = LogFactory.getLog(TablespaceManager.class);

  public static final String DEFAULT_CONFIG_FILE = "storage-default.json";
  public static final String SITE_CONFIG_FILE = "storage-site.json";

  /** default tablespace name */
  public static final String DEFAULT_TABLESPACE_NAME = "default";

  private final static TajoConf systemConf = new TajoConf();
  private final static JSONParser parser = new JSONParser(JSONParser.MODE_JSON_SIMPLE | JSONParser.IGNORE_CONTROL_CHAR);

  // The relation ship among name, URI, Tablespaces must be kept 1:1:1.
  protected static final Map<String, URI> SPACES_URIS_MAP = Maps.newHashMap();
  protected static final TreeMap<URI, Tablespace> TABLE_SPACES = Maps.newTreeMap();

  protected static final Map<Class<?>, Constructor<?>> CONSTRUCTORS = Maps.newHashMap();
  protected static final Map<String, Class<? extends Tablespace>> TABLE_SPACE_HANDLERS = Maps.newHashMap();

  public static final Class[] TABLESPACE_PARAM = new Class[]{String.class, URI.class, JSONObject.class};

  public static final String TABLESPACE_SPEC_CONFIGS_KEY = "configs";

  static {
    instance = new TablespaceManager();
  }

  /**
   * Singleton instance
   */
  private static final TablespaceManager instance;

  private TablespaceManager() {
    initForDefaultConfig(); // loading storage-default.json
    initSiteConfig();       // storage-site.json will override the configs of storage-default.json
    addWarehouseAsSpace();  // adding a warehouse directory for a default tablespace
    addLocalFsTablespace(); // adding a tablespace using local file system by default
  }

  private void addWarehouseAsSpace() {
    Path warehouseDir = TajoConf.getWarehouseDir(systemConf);
    registerTableSpace(DEFAULT_TABLESPACE_NAME, warehouseDir.toUri(), null, true, false);
  }

  private void addLocalFsTablespace() {
    if (TABLE_SPACES.headMap(LOCAL_FS_URI, true).firstEntry() == null && TABLE_SPACE_HANDLERS.containsKey("file")) {
      String tmpName = UUID.randomUUID().toString();
      registerTableSpace(tmpName, LOCAL_FS_URI, null, false, false);
    }
  }

  public static TablespaceManager getInstance() {
    return instance;
  }

  private void initForDefaultConfig() {
    JSONObject json = loadFromConfig(DEFAULT_CONFIG_FILE);
    if (json == null) {
      throw new IllegalStateException("There is no " + SITE_CONFIG_FILE);
    }
    applyConfig(json, false);
  }

  private void initSiteConfig() {
    JSONObject json = loadFromConfig(SITE_CONFIG_FILE);

    // if there is no storage-site.json file, nothing happen.
    if (json != null) {
      applyConfig(json, true);
    }
  }

  private JSONObject loadFromConfig(String fileName) {
    String json;
    try {
      json = JavaResourceUtil.readTextFromResource(fileName);
    } catch (FileNotFoundException fnfe) {
      return null;      
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    return parseJson(json);
  }

  private static JSONObject parseJson(String json) {
    try {
      return (JSONObject) parser.parse(json);
    } catch (ParseException e) {
      throw new RuntimeException(e);
    }
  }

  private void applyConfig(JSONObject json, boolean override) {
    loadStorages(json);
    loadTableSpaces(json, override);
  }

  private void loadStorages(JSONObject json) {
    JSONObject spaces = (JSONObject) json.get(KEY_STORAGES);

    if (spaces != null) {
      Pair<String, Class<? extends Tablespace>> pair = null;
      for (Map.Entry<String, Object> entry : spaces.entrySet()) {

        try {
          pair = extractStorage(entry);
        } catch (ClassNotFoundException e) {
          LOG.warn(e);
          continue;
        }

        TABLE_SPACE_HANDLERS.put(pair.getFirst(), pair.getSecond());
      }
    }
  }

  private Pair<String, Class<? extends Tablespace>> extractStorage(Map.Entry<String, Object> entry)
      throws ClassNotFoundException {

    String storageType = entry.getKey();
    JSONObject storageDesc = (JSONObject) entry.getValue();
    String handlerClass = (String) storageDesc.get(KEY_STORAGE_HANDLER);

    return new Pair<>(
            storageType, (Class<? extends Tablespace>) Class.forName(handlerClass));
  }

  private void loadTableSpaces(JSONObject json, boolean override) {
    JSONObject spaces = (JSONObject) json.get(KEY_SPACES);

    if (spaces != null) {
      for (Map.Entry<String, Object> entry : spaces.entrySet()) {
        JSONObject spaceDetail = (JSONObject) entry.getValue();
        AddTableSpace(
            entry.getKey(),
            URI.create(spaceDetail.getAsString("uri")),
            Boolean.parseBoolean(spaceDetail.getAsString("default")),
            (JSONObject) spaceDetail.get(TABLESPACE_SPEC_CONFIGS_KEY),
            override);
      }
    }
  }

  public static void AddTableSpace(String spaceName, URI uri, boolean isDefault, JSONObject configs, boolean override) {


    if (isDefault) {
      registerTableSpace(DEFAULT_TABLESPACE_NAME, uri, configs, true, override);
    }
    registerTableSpace(spaceName, uri, configs, true, override);
  }

  private static Tablespace registerTableSpace(String spaceName, URI uri, JSONObject spaceDesc,
                                         boolean visible, boolean override) {
    Tablespace tableSpace = initializeTableSpace(spaceName, uri, spaceDesc);
    tableSpace.setVisible(visible);

    try {
      tableSpace.init(systemConf);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    putTablespace(tableSpace, override);

    // If the arbitrary path is allowed, root uri is also added as a tablespace
    if (tableSpace.getProperty().isArbitraryPathAllowed()) {
      URI rootUri = tableSpace.getRootUri();
      // if there already exists or the rootUri is 'file:/', it won't overwrite the tablespace.
      if (!TABLE_SPACES.containsKey(rootUri) && !rootUri.toString().startsWith(LOCAL_FS_URI.toString())) {
        String tmpName = UUID.randomUUID().toString();
        registerTableSpace(tmpName, rootUri, spaceDesc, false, override);
      }
    }
    return tableSpace;
  }

  private static void putTablespace(Tablespace space, boolean override) {
    // It is a device to keep the relationship among name, URI, and tablespace 1:1:1.

    boolean nameExist = SPACES_URIS_MAP.containsKey(space.getName());
    boolean uriExist = TABLE_SPACES.containsKey(space.uri);

    boolean mismatch = nameExist && !SPACES_URIS_MAP.get(space.getName()).equals(space.getUri());
    mismatch = mismatch || uriExist && TABLE_SPACES.get(space.uri).equals(space);

    if (!override && mismatch) {
      throw new RuntimeException("Name or URI of Tablespace must be unique.");
    }

    SPACES_URIS_MAP.put(space.getName(), space.getUri());
    // We must guarantee that the same uri results in the same tablespace instance.
    TABLE_SPACES.put(space.getUri(), space);
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

  public static final String KEY_STORAGES = "storages"; // storages
  public static final String KEY_STORAGE_HANDLER = "handler"; // storages/?/handler
  public static final String KEY_STORAGE_DEFAULT_FORMAT = "default-format"; // storages/?/default-format

  public static final String KEY_SPACES = "spaces";

  private static Tablespace initializeTableSpace(String spaceName, URI uri, JSONObject spaceDesc) {
    final String scheme = UriUtil.getScheme(uri);
    Class<? extends Tablespace> clazz = TABLE_SPACE_HANDLERS.get(scheme);

    if (clazz == null) {
      throw new TajoRuntimeException(new UndefinedTablespaceHandlerException(scheme));
    }

    try {
      Constructor<? extends Tablespace> constructor =
          (Constructor<? extends Tablespace>) CONSTRUCTORS.get(clazz);

      if (constructor == null) {
        constructor = clazz.getDeclaredConstructor(TABLESPACE_PARAM);
        constructor.setAccessible(true);
        CONSTRUCTORS.put(clazz, constructor);
      }

      return constructor.newInstance(new Object[]{spaceName, uri, spaceDesc});
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @VisibleForTesting
  public static Optional<Tablespace> addTableSpaceForTest(Tablespace space) {
    Tablespace existing;
    synchronized (SPACES_URIS_MAP) {

      String scheme = UriUtil.getScheme(space.getUri());
      if (!TABLE_SPACE_HANDLERS.containsKey(scheme)) {
        TABLE_SPACE_HANDLERS.put(scheme, space.getClass());
      }

      // Remove existing one
      SPACES_URIS_MAP.remove(space.getName());
      existing = TABLE_SPACES.remove(space.getUri());

      // Add anotherone for test
      Tablespace tablespace = registerTableSpace(space.name, space.uri, space.getConfig(), true, true);
      try {
        //override conf for test
        if (space.conf != null)
          tablespace.init(space.conf);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    // if there is an existing one, return it.
    return Optional.ofNullable(existing);
  }

  @VisibleForTesting
  public static Optional<Tablespace> removeTablespaceForTest(String name) {
    Tablespace existing = null;
    synchronized (SPACES_URIS_MAP) {
      URI uri = SPACES_URIS_MAP.remove(name);
      if (uri != null) {
        existing = TABLE_SPACES.remove(uri);
      }
    }
    return Optional.ofNullable(existing);
  }

  public Iterable<String> getSupportSchemes() {
    return TABLE_SPACE_HANDLERS.keySet();
  }

  /**
   * Get tablespace for the given URI. If uri is null, the default tablespace will be returned
   *
   * @param uri Table or Table Fragment URI.
   * @param <T> Tablespace class type
   * @return Tablespace. If uri is null, the default tablespace will be returned.
   */
  public static <T extends Tablespace> T get(@Nullable String uri) {

    if (uri == null || uri.isEmpty()) {
      return getDefault();
    }

    Tablespace lastOne = null;

    // Find the longest matched one. For example, assume that the caller tries to find /x/y/z, and
    // there are /x and /x/y. In this case, /x/y will be chosen because it is more specific.
    for (Map.Entry<URI, Tablespace> entry : TABLE_SPACES.headMap(URI.create(uri), true).entrySet()) {
      if (uri.startsWith(entry.getKey().toString())) {
        lastOne = entry.getValue();
      }
    }

    if (lastOne == null) {
      lastOne = initializeTableSpace(UUID.randomUUID().toString(), URI.create(uri), new JSONObject());
      try {
        lastOne.init(systemConf);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    return (T) lastOne;
  }

  /**
   * Get tablespace for the given URI. If uri is null, the default tablespace will be returned
   *
   * @param uri Table or Table Fragment URI.
   * @param <T> Tablespace class type
   * @return Tablespace. If uri is null, the default tablespace will be returned.
   */
  public static <T extends Tablespace> T get(@Nullable URI uri) {
    if (uri == null) {
      return getDefault();
    } else {
      return (T) get(uri.toString());
    }
  }

  /**
   * It returns the default tablespace. This method ensures that it always return the tablespace.
   *
   * @return
   */
  public static <T extends Tablespace> T getDefault() {
    return (T) getByName(DEFAULT_TABLESPACE_NAME);
  }

  public static <T extends Tablespace> T getLocalFs() {
    return (T) get(LOCAL_FS_URI);
  }

  public static <T extends Tablespace> T getByName(String name) {
    URI uri = SPACES_URIS_MAP.get(name);
    if (uri != null) {
      return (T) TABLE_SPACES.get(uri);
    } else {
      throw new TajoRuntimeException(new UndefinedTablespaceException(name));
    }
  }

  public static Optional<? extends Tablespace> getAnyByScheme(String scheme) {
    for (Map.Entry<URI, Tablespace> entry : TABLE_SPACES.entrySet()) {
      String uriScheme = entry.getKey().getScheme();
      if (uriScheme != null && uriScheme.equalsIgnoreCase(scheme)) {
        return Optional.of(entry.getValue());
      }
    }

    return Optional.empty();
  }

  @Override
  public URI getTableURI(@Nullable String spaceName, TableMeta meta, String databaseName, String tableName) {
    Tablespace space = spaceName == null ? getDefault() : getByName(spaceName);
    return space.getTableUri(meta, databaseName, tableName);
  }

  @Override
  public long getTableVolumn(TableDesc table, Optional<EvalNode> filter)
      throws UnsupportedException {
    return get(table.getUri()).getTableVolume(table, filter);
  }

  public static Iterable<Tablespace> getAllTablespaces() {
    return TABLE_SPACES.values();
  }

  public static Collection<MetadataProvider> getMetadataProviders() {
    Collection<Tablespace> filteredSpace = Collections2.filter(TABLE_SPACES.values(), new Predicate<Tablespace>() {
      @Override
      public boolean apply(@Nullable Tablespace space) {
        return space.getProperty().isMetadataProvided();
      }
    });

    return Collections2.transform(filteredSpace, new Function<Tablespace, MetadataProvider>() {
      @Override
      public MetadataProvider apply(@Nullable Tablespace space) {
        return space.getMetadataProvider();
      }
    });
  }
}
