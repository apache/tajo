/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tajo.algebra;

import tajo.util.TUtil;

import java.util.Map;

public class CreateTable extends Expr {
  private String rel_name;
  private ColumnDefinition [] table_elements;
  private String storage_type;
  private String location;
  private Expr subquery;
  private Map<String, String> params;

  public CreateTable(final String tableName) {
    super(ExprType.CreateTable);
    this.rel_name = tableName;
  }

  public CreateTable(final String relationName, final Expr subQuery) {
    this(relationName);
    this.subquery = subQuery;
  }

  public String getRelationName() {
    return this.rel_name;
  }

  public boolean hasLocation() {
    return location != null;
  }

  public String getLocation() {
    return this.location;
  }

  public void setLocation(String location) {
    this.location = location;
  }

  public boolean hasTableElements() {
    return this.table_elements != null;
  }

  public ColumnDefinition [] getTableElements() {
    return table_elements;
  }

  public void setTableElements(ColumnDefinition [] tableElements) {
    this.table_elements = tableElements;
  }

  public boolean hasStorageType() {
    return storage_type != null;
  }

  public void setStorageType(String storageType) {
    this.storage_type = storageType;
  }

  public String getStorageType() {
    return storage_type;
  }

  public boolean hasParams() {
    return params != null;
  }

  public Map<String, String> getParams() {
    return params;
  }

  public void setParams(Map<String, String> params) {
    this.params = params;
  }

  public boolean hasSubQuery() {
    return subquery != null;
  }

  public Expr getSubQuery() {
    return subquery;
  }

  @Override
  boolean equalsTo(Expr expr) {
    CreateTable another = (CreateTable) expr;
    return rel_name.equals(another.rel_name) &&
        TUtil.checkEquals(table_elements, another.table_elements) &&
        TUtil.checkEquals(storage_type, another.storage_type) &&
        TUtil.checkEquals(location, another.location) &&
        TUtil.checkEquals(subquery, another.subquery) &&
        TUtil.checkEquals(params, another.params);
  }

  public static class ColumnDefinition {
    String col_name;
    String data_type;

    public ColumnDefinition(String columnName, String dataType) {
      this.col_name = columnName;
      this.data_type = dataType;
    }

    public String getColumnName() {
      return this.col_name;
    }

    public String getDataType() {
      return this.data_type;
    }

    public boolean equals(Object obj) {
      if (obj instanceof ColumnDefinition) {
        ColumnDefinition another = (ColumnDefinition) obj;
        return col_name.equals(another.col_name) && data_type.equals(another.data_type);
      }

      return false;
    }
  }
}
