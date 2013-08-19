/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.engine.planner;

import com.google.gson.annotations.Expose;
import org.apache.tajo.json.GsonObject;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.engine.json.CoreGsonHelper;
import org.apache.tajo.util.TUtil;

public class FromTable implements Cloneable, GsonObject {
  @Expose private TableDesc desc;
  @Expose private String alias = null;

  public FromTable() {}

  public FromTable(final TableDesc desc) {
    this.desc = desc;
  }

  public FromTable(final TableDesc desc, final String alias) {
    this(desc);
    this.alias = alias;
  }

  public TableDesc getTableDesc() {
    return this.desc;
  }

  public final String getTableName() {
    return desc.getName();
  }

  public final String getTableId() {
    return alias == null ? desc.getName() : alias;
  }

  public final CatalogProtos.StoreType getStoreType() {
    return desc.getMeta().getStoreType();
  }

  public final Schema getSchema() {
    return desc.getMeta().getSchema();
  }

  public final void setAlias(String alias) {
    this.alias = alias;
  }

  public final String getAlias() {
    return alias;
  }

  public final boolean hasAlias() {
    return alias != null;
  }

  public final String toString() {
    if (alias != null)
      return desc.getName() + " as " + alias;
    else
      return desc.getName();
  }

  public boolean equals(Object obj) {
    if (obj instanceof FromTable) {
      FromTable other = (FromTable) obj;
      return this.desc.equals(other.desc)
          && TUtil.checkEquals(this.alias, other.alias);
    } else {
      return false;
    }
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    FromTable table = (FromTable) super.clone();
    table.desc = (TableDesc) desc.clone();
    table.alias = alias;

    return table;
  }

  @Override
  public String toJson() {
    return CoreGsonHelper.toJson(this, FromTable.class);
  }
}
