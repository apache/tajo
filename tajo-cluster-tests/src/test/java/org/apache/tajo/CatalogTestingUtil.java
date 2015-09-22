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

package org.apache.tajo;

import org.apache.tajo.annotation.NotNull;
import org.apache.tajo.annotation.Nullable;
import org.apache.tajo.catalog.CatalogConstants;
import org.apache.tajo.catalog.store.*;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.exception.UnsupportedCatalogStore;

public class CatalogTestingUtil {

  public static TajoConf configureCatalog(TajoConf conf, String testDirPath) throws UnsupportedCatalogStore {

    String driverClassName = System.getProperty(CatalogConstants.STORE_CLASS);
    final boolean useDefaultCatalog = driverClassName == null;

    conf = initializeDerbyStore(conf, testDirPath);

    // TODO: if useDefaultCatalog is false, use external database as catalog
    return conf;
  }

  static <T extends CatalogStore> boolean requireAuth(Class<T> clazz) {
    return clazz.equals(MySQLStore.class) ||
        clazz.equals(MariaDBStore.class) ||
        clazz.equals(PostgreSQLStore.class) ||
        clazz.equals(OracleStore.class);
  }

  private static TajoConf initializeDerbyStore(TajoConf conf, String testDirPath) throws UnsupportedCatalogStore {
    return configureCatalogClassAndUri(conf, DerbyStore.class, getInmemoryDerbyCatalogURI(testDirPath));
  }

  private static <T extends CatalogStore> TajoConf configureCatalogClassAndUri(TajoConf conf,
                                                                               Class<T> catalogClass,
                                                                               String catalogUri) {
    conf.set(CatalogConstants.STORE_CLASS, catalogClass.getCanonicalName());
    conf.set(CatalogConstants.CATALOG_URI, catalogUri);
    conf.setVar(ConfVars.CATALOG_ADDRESS, "localhost:0");
    return conf;
  }

  private static String getInmemoryDerbyCatalogURI(String testDirPath) throws UnsupportedCatalogStore {
    return getCatalogURI(DerbyStore.class, "memory", testDirPath);
  }

  private static <T extends CatalogStore> String getCatalogURI(@NotNull Class<T> clazz,
                                                               @Nullable String schemeSpecificPart,
                                                               @NotNull String testDirPath)
      throws UnsupportedCatalogStore {
    String uriScheme = getCatalogURIScheme(clazz);
    StringBuilder sb = new StringBuilder("jdbc:").append(uriScheme).append(":");
    if (schemeSpecificPart != null) {
      sb.append(schemeSpecificPart).append(":");
    }
    sb.append(testDirPath).append("/db;create=true");
    return sb.toString();
  }

  private static <T extends CatalogStore> String getCatalogURIScheme(Class<T> clazz) throws UnsupportedCatalogStore {
    if (clazz.equals(DerbyStore.class)) {
      return "derby";
    } else if (clazz.equals(MariaDBStore.class)) {
      return "mariadb";
    } else if (clazz.equals(MySQLStore.class)) {
      return "mysql";
    } else if (clazz.equals(OracleStore.class)) {
      return "oracle";
    } else if (clazz.equals(PostgreSQLStore.class)) {
      return "postgresql";
    } else {
      throw new UnsupportedCatalogStore(clazz.getCanonicalName());
    }
  }
}
