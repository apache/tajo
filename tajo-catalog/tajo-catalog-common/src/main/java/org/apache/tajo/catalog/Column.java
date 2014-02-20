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

package org.apache.tajo.catalog;

import com.google.common.base.Objects;
import com.google.gson.annotations.Expose;
import org.apache.tajo.catalog.json.CatalogGsonHelper;
import org.apache.tajo.catalog.proto.CatalogProtos.ColumnProto;
import org.apache.tajo.common.ProtoObject;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.common.TajoDataTypes.DataType;
import org.apache.tajo.json.GsonObject;

/**
 * Describes a column. It is an immutable object.
 */
public class Column implements ProtoObject<ColumnProto>, GsonObject {
	@Expose protected String name;
	@Expose protected DataType dataType;

  /**
   *
   * @param name Column name
   * @param dataType Data Type with length
   */
	public Column(String name, DataType dataType) {
    this.name = CatalogUtil.normalizeIdentifier(name);
		this.dataType = dataType;
	}

  /**
   *
   * @param name Column name
   * @param type Data Type without length
   */
  public Column(String name, TajoDataTypes.Type type) {
    this(name, CatalogUtil.newSimpleDataType(type));
  }

  /**
   *
   * @param name Column name
   * @param type Data Type
   * @param typeLength The length of type
   */
  public Column(String name, TajoDataTypes.Type type, int typeLength) {
    this(name, CatalogUtil.newDataTypeWithLen(type, typeLength));
  }

	public Column(ColumnProto proto) {
    name = CatalogUtil.normalizeIdentifier(proto.getName());
    dataType = proto.getDataType();
	}

  /**
   *
   * @return True if a column includes a table name. Otherwise, it returns False.
   */
  public boolean hasQualifier() {
    return name.split(CatalogUtil.IDENTIFIER_DELIMITER_REGEXP).length > 1;
  }

  /**
   *
   * @return The full name of this column.
   */
	public String getQualifiedName() {
    return name;
	}

  /**
   *
   * @return The qualifier
   */
  public String getQualifier() {
    return CatalogUtil.extractQualifier(name);
  }

  /**
   *
   * @return The simple name without qualifications
   */
  public String getSimpleName() {
    return CatalogUtil.extractSimpleName(name);
  }

  /**
   *
   * @return DataType which includes domain type and scale.
   */
	public DataType getDataType() {
		return this.dataType;
	}
	
	@Override
	public boolean equals(Object o) {
		if (o instanceof Column) {
			Column another = (Column)o;
			return name.equals(another.name) && dataType.equals(another.dataType);
    }
		return false;
	}
	
  public int hashCode() {
    return Objects.hashCode(name, dataType);

  }
  
  @Override
  public Object clone() {
    return this;
  }

  /**
   *
   * @return The protocol buffer object for Column
   */
	@Override
	public ColumnProto getProto() {
    return ColumnProto.newBuilder().setName(this.name).setDataType(this.dataType).build();
	}
	
	public String toString() {
    StringBuilder sb = new StringBuilder(getQualifiedName());
    sb.append(" (").append(getDataType().getType());
    if (getDataType().getLength()  > 0) {
      sb.append("(" + getDataType().getLength() + ")");
    }
    sb.append(")");
	  return sb.toString();
	}

  @Override
	public String toJson() {
		return CatalogGsonHelper.toJson(this, Column.class);
	}

}