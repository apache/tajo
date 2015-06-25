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

package org.apache.tajo.plan;

import org.apache.tajo.OverridableConf;
import org.apache.tajo.SessionVars;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.plan.logical.CreateTableNode;
import org.apache.tajo.plan.logical.ScanNode;
import org.apache.tajo.storage.StorageConstants;
import org.apache.tajo.util.KeyValueSet;

/**
 * An utility class for table property
 */
public class TablePropertyUtil {
  /**
   * It sets default table property for newly created table
   *
   * @param context QueryContext
   * @param node CreateTableNode
   */
  public static void setTableProperty(OverridableConf context, CreateTableNode node) {
    String storeType = node.getStorageType();
    KeyValueSet property = node.getOptions();

    if (storeType.equalsIgnoreCase("CSV") || storeType.equalsIgnoreCase("TEXT")) {
      setSessionToProperty(context, SessionVars.NULL_CHAR, property, StorageConstants.TEXT_NULL);
    }

    setSessionToProperty(context, SessionVars.TIMEZONE, property, StorageConstants.TIMEZONE);
  }

  private static void setSessionToProperty(OverridableConf context,
                                           SessionVars sessionVarKey,
                                           KeyValueSet property,
                                           String propertyKey) {

    if (context.containsKey(sessionVarKey)) {
      property.set(propertyKey, context.get(sessionVarKey));
    }
  }

  /**
   * It sets default table properties affected by system global configuration
   * The table property are implicitly used to read Table rows.
   *
   * @param context QueryContext
   * @param node ScanNode
   */
  public static void setTableProperty(OverridableConf context, ScanNode node) {
    TableMeta meta = node.getTableDesc().getMeta();

    setProperty(context, SessionVars.TIMEZONE, meta, StorageConstants.TIMEZONE);
    setProperty(context, SessionVars.NULL_CHAR, meta, StorageConstants.TEXT_NULL);
  }

  /**
   * If there is no table property for the propertyKey, set default property to the table.
   * If session variable is set, it is set to the table property. Otherwise, the default property
   * in the system conf will be used.
   *
   * @param context QueryContext
   * @param sessionVarKey session variable key
   * @param meta TableMeta
   * @param propertyKey table property key
   */
  private static void setProperty(OverridableConf context, SessionVars sessionVarKey,
                          TableMeta meta, String propertyKey) {

    if (!meta.containsOption(propertyKey)) {
      meta.putOption(propertyKey, context.get(sessionVarKey));
    }
  }
}
