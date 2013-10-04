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
import org.apache.tajo.util.TUtil;

/**
 * It represents a column. It is usually used for relations.
 */
public class Column implements ProtoObject<ColumnProto>, Cloneable, GsonObject {
	private ColumnProto.Builder builder = null;

  @Expose protected String qualifier; // optional
	@Expose protected String name; // required
	@Expose protected DataType dataType; // required
	
	public Column() {
		this.builder = ColumnProto.newBuilder();
	}
	  
	public Column(String columnName, DataType dataType) {
	  this();
		checkAndSetName(columnName.toLowerCase());
		this.dataType = dataType;
	}

  public Column(String columnName, TajoDataTypes.Type type) {
    this(columnName, CatalogUtil.newSimpleDataType(type));
  }

  public Column(String columnName, TajoDataTypes.Type type, int typeLength) {
    this(columnName, CatalogUtil.newDataTypeWithLen(type, typeLength));
  }
	
	public Column(ColumnProto proto) {
    this();
    name = proto.getColumnName().toLowerCase();
    dataType = proto.getDataType();
    if (proto.hasQualifier()) {
      qualifier = proto.getQualifier().toLowerCase();
    }
	}

  private void checkAndSetName(String qualifiedOrName) {
    String [] splits = qualifiedOrName.split("\\.");
    if (splits.length > 1) {
      qualifier = qualifiedOrName.substring(0, qualifiedOrName.lastIndexOf("."));
      name = qualifiedOrName.substring(qualifiedOrName.lastIndexOf(".") + 1, qualifiedOrName.length());
    } else {
      qualifier = null;
      name = qualifiedOrName;
    }
  }

	public String getQualifiedName() {
    if (qualifier != null) {
      return qualifier + "." + name;
    } else {
      return name;
    }
	}
	
  public boolean hasQualifier() {
    return qualifier != null;
  }

  public void setQualifier(String qualifier) {
    this.qualifier = qualifier.toLowerCase();
  }

  public String getQualifier() {
    if (qualifier != null) {
      return qualifier;
    } else {
      return "";
    }    
  }

  public String getColumnName() {
    return name;
  }
	
	public void setName(String name) {
    checkAndSetName(name.toLowerCase());
	}
	
	public DataType getDataType() {
		return this.dataType;
	}
	
	public void setDataType(DataType dataType) {
		this.dataType = dataType;
	}
	
	@Override
	public boolean equals(Object o) {
		if (o instanceof Column) {
			Column another = (Column)o;
			return name.equals(another.name) &&
          dataType.equals(another.dataType) &&
          TUtil.checkEquals(qualifier, another.qualifier);
    }
		return false;
	}
	
  public int hashCode() {
    return Objects.hashCode(name, dataType, qualifier);

  }
  
  @Override
  public Object clone() throws CloneNotSupportedException {
    Column column = (Column) super.clone();
    column.builder = ColumnProto.newBuilder();
    column.name = name;
    column.dataType = dataType;
    column.qualifier = qualifier != null ? qualifier : null;
    return column;
  }

	@Override
	public ColumnProto getProto() {
    builder.setColumnName(this.name);
    builder.setDataType(this.dataType);
    if (qualifier != null) {
      builder.setQualifier(qualifier);
    }

    return builder.build();
	}
	
	public String toString() {
	  return getQualifiedName() +" (" + getDataType().getType() +"(" + getDataType().getLength() + "))";
	}

  @Override
	public String toJson() {
		return CatalogGsonHelper.toJson(this, Column.class);
	}
}