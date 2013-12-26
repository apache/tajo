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

package org.apache.tajo.algebra;

import org.apache.tajo.util.TUtil;

import java.util.List;
import java.util.Map;

public class CreateTable extends Expr {
  private boolean external = false;
  private String tableName;
  private ColumnDefinition [] tableElements;
  private String storageType;
  private String location;
  private Expr subquery;
  private Map<String, String> params;
  private PartitionDescExpr partition;

  public CreateTable(final String tableName) {
    super(OpType.CreateTable);
    this.tableName = tableName;
  }

  public CreateTable(final String tableName, final Expr subQuery) {
    this(tableName);
    this.subquery = subQuery;
  }

  public void setExternal() {
    external = true;
  }

  public boolean isExternal() {
    return external;
  }

  public String getTableName() {
    return this.tableName;
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
    return this.tableElements != null;
  }

  public ColumnDefinition [] getTableElements() {
    return tableElements;
  }

  public void setTableElements(ColumnDefinition [] tableElements) {
    this.tableElements = tableElements;
  }

  public boolean hasStorageType() {
    return storageType != null;
  }

  public void setStorageType(String storageType) {
    this.storageType = storageType;
  }

  public String getStorageType() {
    return storageType;
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

  public boolean hasPartition() {
    return partition != null;
  }

  public void setPartition(PartitionDescExpr partition) {
    this.partition = partition;
  }

  public <T extends PartitionDescExpr> T getPartition() {
    return (T) this.partition;
  }

  public boolean hasSubQuery() {
    return subquery != null;
  }

  public void setSubQuery(Expr subquery) {
    this.subquery = subquery;
  }

  public Expr getSubQuery() {
    return subquery;
  }

  @Override
  boolean equalsTo(Expr expr) {
    CreateTable another = (CreateTable) expr;
    return tableName.equals(another.tableName) &&
        TUtil.checkEquals(tableElements, another.tableElements) &&
        TUtil.checkEquals(storageType, another.storageType) &&
        TUtil.checkEquals(location, another.location) &&
        TUtil.checkEquals(subquery, another.subquery) &&
        TUtil.checkEquals(params, another.params);
  }

  public static class ColumnDefinition extends DataTypeExpr {
    String col_name;

    public ColumnDefinition(String columnName, String dataType) {
      super(dataType);
      this.col_name = columnName;
    }

    public ColumnDefinition(String columnName, DataTypeExpr dataType) {
      super(dataType.getTypeName());
      if (dataType.hasLengthOrPrecision()) {
        setLengthOrPrecision(dataType.lengthOrPrecision);
        if (dataType.hasScale()) {
          setScale(dataType.scale);
        }
      }
      this.col_name = columnName;
    }

    public String getColumnName() {
      return this.col_name;
    }

    public boolean equals(Object obj) {
      if (obj instanceof ColumnDefinition) {
        ColumnDefinition another = (ColumnDefinition) obj;
        return col_name.equals(another.col_name) && super.equals(another);
      }

      return false;
    }
  }

  public static enum PartitionType {
    RANGE,
    HASH,
    LIST,
    COLUMN
  }

  public static abstract class PartitionDescExpr {
    PartitionType type;

    public PartitionDescExpr(PartitionType type) {
      this.type = type;
    }

    public PartitionType getPartitionType() {
      return type;
    }
  }

  public static class RangePartition extends PartitionDescExpr {
    ColumnReferenceExpr [] columns;
    List<RangePartitionSpecifier> specifiers;

    public RangePartition(ColumnReferenceExpr [] columns, List<RangePartitionSpecifier> specifiers) {
      super(PartitionType.RANGE);
      this.columns = columns;
      this.specifiers = specifiers;
    }

    public ColumnReferenceExpr [] getColumns() {
      return columns;
    }

    public List<RangePartitionSpecifier> getSpecifiers() {
      return specifiers;
    }
  }

  public static class HashPartition extends PartitionDescExpr {
    ColumnReferenceExpr [] columns;
    Expr quantity;
    List<PartitionSpecifier> specifiers;
    public HashPartition(ColumnReferenceExpr [] columns, Expr quantity) {
      super(PartitionType.HASH);
      this.columns = columns;
      this.quantity = quantity;
    }

    public HashPartition(ColumnReferenceExpr [] columns, List<PartitionSpecifier> specifier) {
      super(PartitionType.HASH);
      this.columns = columns;
      this.specifiers = specifier;
    }

    public ColumnReferenceExpr [] getColumns() {
      return columns;
    }

    public boolean hasQuantifier() {
      return quantity != null;
    }

    public Expr getQuantifier() {
      return quantity;
    }

    public boolean hasSpecifiers() {
      return specifiers != null;
    }

    public List<PartitionSpecifier> getSpecifiers() {
      return specifiers;
    }
  }

  public static class ListPartition extends PartitionDescExpr {
    ColumnReferenceExpr [] columns;
    List<ListPartitionSpecifier> specifiers;

    public ListPartition(ColumnReferenceExpr [] columns, List<ListPartitionSpecifier> specifers) {
      super(PartitionType.LIST);
      this.columns = columns;
      this.specifiers = specifers;
    }

    public ColumnReferenceExpr [] getColumns() {
      return columns;
    }

    public List<ListPartitionSpecifier> getSpecifiers() {
      return specifiers;
    }
  }

  public static class ColumnPartition extends PartitionDescExpr {
    private ColumnDefinition [] columns;
    private boolean isOmitValues;

    public ColumnPartition(ColumnDefinition [] columns, boolean isOmitValues) {
      super(PartitionType.COLUMN);
      this.columns = columns;
      this.isOmitValues = isOmitValues;
    }

    public ColumnDefinition [] getColumns() {
      return columns;
    }

    public boolean isOmitValues() {
      return isOmitValues;
    }
  }

  public static class RangePartitionSpecifier extends PartitionSpecifier {
    Expr end;
    boolean maxValue;

    public RangePartitionSpecifier(String name, Expr end) {
      super(name);
      this.end = end;
    }

    public RangePartitionSpecifier(String name) {
      super(name);
      maxValue = true;
    }

    public Expr getEnd() {
      return end;
    }

    public boolean isEndMaxValue() {
      return this.maxValue;
    }
  }

  public static class ListPartitionSpecifier extends PartitionSpecifier {
    ValueListExpr valueList;

    public ListPartitionSpecifier(String name, ValueListExpr valueList) {
      super(name);
      this.valueList = valueList;
    }

    public ValueListExpr getValueList() {
      return valueList;
    }
  }

  public static class PartitionSpecifier {
    private String name;

    public PartitionSpecifier(String name) {
      this.name = name;
    }

    public String getName() {
      return this.name;
    }
  }
}
