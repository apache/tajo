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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.storage.v2.StorageManagerV2;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class StorageManagerFactory {
  private static Map<String, AbstractStorageManager> storageManagers =
      new HashMap<String, AbstractStorageManager>();

  public static AbstractStorageManager getStorageManager(TajoConf conf) throws IOException {
    return getStorageManager(conf, null);
  }

  public static synchronized AbstractStorageManager getStorageManager (
      TajoConf conf, Path dataRoot) throws IOException {
    return getStorageManager(conf, dataRoot, conf.getBoolean("tajo.storage.manager.v2", false));
  }

  private static synchronized AbstractStorageManager getStorageManager (
      TajoConf conf, Path dataRoot, boolean v2) throws IOException {
    if(dataRoot != null) {
      conf.setVar(TajoConf.ConfVars.ROOT_DIR, dataRoot.toString());
    }

    URI uri;
    if(dataRoot == null) {
      uri = FileSystem.get(conf).getUri();
    } else {
      uri = dataRoot.toUri();
    }
    String key = "file".equals(uri.getScheme()) ? "file" : uri.getScheme() + uri.getHost() + uri.getPort();

    if(v2) {
      key += "_v2";
    }

    if(storageManagers.containsKey(key)) {
      return storageManagers.get(key);
    } else {
      AbstractStorageManager storageManager = null;

      if(v2) {
        storageManager = new StorageManagerV2(conf);
      } else {
        storageManager = new StorageManager(conf);
      }

      storageManagers.put(key, storageManager);

      return storageManager;
    }
  }

  public static synchronized SeekableScanner getSeekableScanner(
      TajoConf conf, TableMeta meta, Fragment fragment, Schema schema) throws IOException {
    return (SeekableScanner)getStorageManager(conf, null, false).getScanner(meta, fragment, schema);
  }

  public static synchronized SeekableScanner getSeekableScanner(
      TajoConf conf, TableMeta meta, Path path) throws IOException {

    FileSystem fs = path.getFileSystem(conf);
    FileStatus status = fs.getFileStatus(path);
    Fragment fragment = new Fragment(path.getName(), path, meta, 0, status.getLen());

    return getSeekableScanner(conf, meta, fragment, fragment.getSchema());
  }
}
