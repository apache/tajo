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

import com.google.common.base.Objects;
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
  private PartitionMethodDescExpr partition;
  private boolean ifNotExists;

  public CreateTable(final String tableName, boolean ifNotExists) {
    super(OpType.CreateTable);
    this.tableName = tableName;
    this.ifNotExists = ifNotExists;
  }

  public CreateTable(final String tableName, final Expr subQuery, boolean ifNotExists) {
    this(tableName, ifNotExists);
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

  public void setPartitionMethod(PartitionMethodDescExpr partition) {
    this.partition = partition;
  }

  public <T extends PartitionMethodDescExpr> T getPartitionMethod() {
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

  public boolean isIfNotExists() {
    return ifNotExists;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(
        external, tableName, Objects.hashCode(tableElements),
        storageType, subquery, location, params, partition, ifNotExists);
  }

  @Override
  boolean equalsTo(Expr expr) {
    CreateTable another = (CreateTable) expr;
    return external == another.external &&
        tableName.equals(another.tableName) &&
        TUtil.checkEquals(tableElements, another.tableElements) &&
        TUtil.checkEquals(storageType, another.storageType) &&
        TUtil.checkEquals(location, another.location) &&
        TUtil.checkEquals(subquery, another.subquery) &&
        TUtil.checkEquals(params, another.params) &&
        TUtil.checkEquals(partition, another.partition) &&
        ifNotExists == another.ifNotExists;
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

    @Override
    public int hashCode() {
      int hash = super.hashCode();
      return hash * 89 * col_name.hashCode();

    }

    @Override
    public boolean equalsTo(Expr expr) {
      if (expr instanceof ColumnDefinition) {
        ColumnDefinition another = (ColumnDefinition) expr;
        return col_name.equals(another.col_name) && super.equalsTo(another);
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

  public static abstract class PartitionMethodDescExpr {
    PartitionType type;

    public PartitionMethodDescExpr(PartitionType type) {
      this.type = type;
    }

    public PartitionType getPartitionType() {
      return type;
    }
  }

  public static class RangePartition extends PartitionMethodDescExpr {
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

    public int hashCode() {
      return Objects.hashCode(type, Objects.hashCode(columns), specifiers);
    }

    public boolean equals(Object object) {
      if (object instanceof RangePartition) {
        RangePartition another = (RangePartition) object;
        return type == another.type && TUtil.checkEquals(columns, another.columns) &&
            specifiers.equals(another.specifiers);
      } else {
        return false;
      }
    }
  }

  public static class HashPartition extends PartitionMethodDescExpr {
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

    public int hashCode() {
      return Objects.hashCode(type, columns, specifiers);
    }

    public boolean equals(Object object) {
      if (object instanceof HashPartition) {
        HashPartition another = (HashPartition) object;
        return type == another.type && TUtil.checkEquals(columns, another.columns) &&
            specifiers.equals(another.specifiers);
      } else {
        return false;
      }
    }
  }

  public static class ListPartition extends PartitionMethodDescExpr {
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

    public int hashCode() {
      return Objects.hashCode(Objects.hashCode(columns), specifiers);
    }

    public boolean equals(Object object) {
      if (object instanceof ListPartition) {
        ListPartition another = (ListPartition) object;
        return type == another.type && TUtil.checkEquals(columns, another.columns) &&
            specifiers.equals(another.specifiers);
      } else {
        return false;
      }
    }
  }

  public static class ColumnPartition extends PartitionMethodDescExpr {
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

    public int hashCode() {
      return Objects.hashCode(Objects.hashCode(columns), isOmitValues);
    }

    public boolean equals(Object object) {
      if (object instanceof ColumnPartition) {
        ColumnPartition another = (ColumnPartition) object;
        return type == another.type && TUtil.checkEquals(columns, another.columns) &&
            TUtil.checkEquals(isOmitValues, another.isOmitValues);
      }
      return false;
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

    public int hashCode() {
      return Objects.hashCode(end, maxValue);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      RangePartitionSpecifier that = (RangePartitionSpecifier) o;

      if (maxValue != that.maxValue) return false;
      if (!end.equals(that.end)) return false;

      return true;
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

    @Override
    public int hashCode() {
      return valueList.hashCode();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      ListPartitionSpecifier that = (ListPartitionSpecifier) o;

      return valueList.equals(that.valueList);
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

    public int hashCode() {
      return name.hashCode();
    }

    public boolean equals(Object obj) {
      if (obj instanceof PartitionSpecifier ) {
        return name.equals(((PartitionSpecifier)obj).name);
      } else {
        return false;
      }
    }
  }
}
