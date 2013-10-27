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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.storage.v2.StorageManagerV2;

import java.io.IOException;
import java.net.URI;
import java.util.Map;

import static org.apache.tajo.conf.TajoConf.ConfVars;

public class StorageManagerFactory {
  private static final Map<String, AbstractStorageManager> storageManagers = Maps.newHashMap();

  public static AbstractStorageManager getStorageManager(TajoConf conf) throws IOException {
    return getStorageManager(conf, null);
  }

  public static synchronized AbstractStorageManager getStorageManager (
      TajoConf conf, Path warehouseDir) throws IOException {
    return getStorageManager(conf, warehouseDir, conf.getBoolVar(ConfVars.STORAGE_MANAGER_VERSION_2));
  }

  private static synchronized AbstractStorageManager getStorageManager (
      TajoConf conf, Path warehouseDir, boolean v2) throws IOException {

    URI uri;
    TajoConf localConf = new TajoConf(conf);
    if (warehouseDir != null) {
      localConf.setVar(ConfVars.WAREHOUSE_DIR, warehouseDir.toUri().toString());
    }

    uri = TajoConf.getWarehouseDir(localConf).toUri();

    String key = "file".equals(uri.getScheme()) ? "file" : uri.toString();

    if(v2) {
      key += "_v2";
    }

    if(storageManagers.containsKey(key)) {
      AbstractStorageManager sm = storageManagers.get(key);
      return sm;
    } else {
      AbstractStorageManager storageManager;

      if(v2) {
        storageManager = new StorageManagerV2(localConf);
      } else {
        storageManager = new StorageManager(localConf);
      }

      storageManagers.put(key, storageManager);

      return storageManager;
    }
  }

  public static synchronized SeekableScanner getSeekableScanner(
      TajoConf conf, TableMeta meta, Schema schema, Fragment fragment, Schema target) throws IOException {
    return (SeekableScanner)getStorageManager(conf, null, false).getScanner(meta, schema, fragment, target);
  }

  public static synchronized SeekableScanner getSeekableScanner(
      TajoConf conf, TableMeta meta, Schema schema, Path path) throws IOException {

    FileSystem fs = path.getFileSystem(conf);
    FileStatus status = fs.getFileStatus(path);
    Fragment fragment = new Fragment(path.getName(), path, 0, status.getLen());

    return getSeekableScanner(conf, meta, schema, fragment, schema);
  }
}
