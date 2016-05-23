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
import org.apache.tajo.schema.IdentifierUtil;
import org.apache.tajo.type.Type;
import org.apache.tajo.type.TypeProtobufEncoder;

/**
 * Describes a column. It is an immutable object.
 */
@Deprecated
public class Column implements ProtoObject<ColumnProto>, GsonObject {
	@Expose protected String name;
  @Expose protected Type type;

  /**
   * Column Constructor
   *
   * @param name field name
   * @param type Type description
   */
  public Column(String name, TypeDesc type) {
    this.name = name;
    this.type = TypeConverter.convert(type);
  }

  /**
   *
   * @param name Column name
   * @param dataType Data Type with length
   */
	public Column(String name, DataType dataType) {
    this(name, new TypeDesc(dataType));
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
   * @param type Type
   */
  public Column(String name, Type type) {
    this.name = name;
    this.type = type;
  }

	public Column(ColumnProto proto) {
    name = proto.getName();
    type = TypeProtobufEncoder.decode(proto.getType());
	}

  /**
   *
   * @return True if a column includes a table name. Otherwise, it returns False.
   */
  public boolean hasQualifier() {
    return name.split(IdentifierUtil.IDENTIFIER_DELIMITER_REGEXP).length > 1;
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
    return IdentifierUtil.extractQualifier(name);
  }

  /**
   *
   * @return The simple name without qualifications
   */
  public String getSimpleName() {
    return IdentifierUtil.extractSimpleName(name);
  }

  /**
   * Return type description
   *
   * @return TypeDesc
   */
  public TypeDesc getTypeDesc() {
    return TypeConverter.convert(this.type);
  }

  /**
   * @return Type which includes domain type and scale.
   */
  public Type getType() {
    return this.type;
  }

  /**
   *
   * @return DataType which includes domain type and scale.
   */
	public DataType getDataType() {
		return TypeConverter.convert(this.type).getDataType();
	}
	
	@Override
	public boolean equals(Object o) {
		if (o instanceof Column) {
			Column another = (Column)o;
			return name.equals(another.name) && type.equals(another.type);
    }
		return false;
	}
	
  public int hashCode() {
    return Objects.hashCode(name, type);
  }

  /**
   *
   * @return The protocol buffer object for Column
   */
	@Override
	public ColumnProto getProto() {
    ColumnProto.Builder builder = ColumnProto.newBuilder();
    builder
        .setName(this.name)
        .setType(this.type.getProto());
    return builder.build();
	}
	
	public String toString() {
    StringBuilder sb = new StringBuilder(getQualifiedName());
    sb.append(" (").append(type.toString()).append(")");
	  return sb.toString();
	}

  @Override
	public String toJson() {
		return CatalogGsonHelper.toJson(this, Column.class);
	}

}