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

import com.google.gson.annotations.Expose;
import org.apache.tajo.json.GsonObject;
import org.apache.tajo.catalog.json.CatalogGsonHelper;
import org.apache.tajo.catalog.proto.CatalogProtos.ColumnProto;
import org.apache.tajo.common.ProtoObject;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.common.TajoDataTypes.DataType;

public class Column implements ProtoObject<ColumnProto>, Cloneable, GsonObject {
	private ColumnProto.Builder builder = null;
	
	@Expose protected String name; // required
	@Expose protected DataType dataType; // required
	
	public Column() {
		this.builder = ColumnProto.newBuilder();
	}
	  
	public Column(String columnName, DataType dataType) {
	  this();
		this.name = columnName.toLowerCase();
		this.dataType = dataType;
	}

  public Column(String columnName, TajoDataTypes.Type type) {
    this(columnName, CatalogUtil.newDataTypeWithoutLen(type));
  }

  public Column(String columnName, TajoDataTypes.Type type, int typeLength) {
    this(columnName, CatalogUtil.newDataTypeWithLen(type, typeLength));
  }
	
	public Column(ColumnProto proto) {
		this(proto.getColumnName(), proto.getDataType());
	}

	public String getQualifiedName() {
		return this.name;
	}
	
  public boolean isQualified() {
    return getQualifiedName().split("\\.").length == 2;
  }

  public String getTableName() {
    if (isQualified()) {
      return getQualifiedName().split("\\.")[0];
    } else {
      return "";
    }    
  }

  public String getColumnName() {
    if (isQualified())
      return getQualifiedName().split("\\.")[1];
    else
      return getQualifiedName();
  }
	
	public void setName(String name) {
		this.name = name.toLowerCase();
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
			Column cd = (Column)o;
			return this.getQualifiedName().equals(cd.getQualifiedName()) &&
					this.getDataType().equals(cd.getDataType());
		}
		return false;
	}
	
  public int hashCode() {
    return getQualifiedName().hashCode() ^ (getDataType().hashCode() * 17);
  }
  
  @Override
  public Object clone() throws CloneNotSupportedException {
    Column column = (Column) super.clone();
    column.builder = ColumnProto.newBuilder();
    column.name = name;
    column.dataType = dataType;
    return column;
  }

	@Override
	public ColumnProto getProto() {
    builder.setColumnName(this.name);
    builder.setDataType(this.dataType);

    return builder.build();
	}
	
	public String toString() {
	  return getQualifiedName() +" (" + getDataType().getType() +")";
	}

  @Override
	public String toJson() {
		return CatalogGsonHelper.toJson(this, Column.class);
	}
}