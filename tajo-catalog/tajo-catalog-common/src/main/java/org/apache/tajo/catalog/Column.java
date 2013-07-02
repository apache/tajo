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
import org.apache.tajo.catalog.json.GsonCreator;
import org.apache.tajo.catalog.proto.CatalogProtos.ColumnProto;
import org.apache.tajo.catalog.proto.CatalogProtos.ColumnProtoOrBuilder;
import org.apache.tajo.common.ProtoObject;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.common.TajoDataTypes.DataType;

public class Column implements ProtoObject<ColumnProto>, Cloneable {
	private ColumnProto proto = ColumnProto.getDefaultInstance();
	private ColumnProto.Builder builder = null;
	private boolean viaProto = false;
	
	@Expose protected String name;
	@Expose protected DataType dataType;
	
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
	
	public Column(ColumnProto proto) {
		this.proto = proto;
		this.viaProto = true;
	}
	

	public String getQualifiedName() {
		ColumnProtoOrBuilder p = viaProto ? proto : builder;
		if(name != null) {
			return this.name;
		}
		if(!p.hasColumnName()) {
			return null;			
		}		
		this.name = p.getColumnName();
		
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
	  setModified();
		this.name = name.toLowerCase();
	}
	
	public DataType getDataType() {
		ColumnProtoOrBuilder p = viaProto ? proto : builder;
		if(dataType != null) {
			return this.dataType;
		}
		if(!p.hasDataType()) {
			return null;
		}
		this.dataType = p.getDataType();
		
		return this.dataType;
	}
	
	public void setDataType(DataType dataType) {
		setModified();
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
    initFromProto();
    column.proto = null;
    column.viaProto = false;
    column.builder = ColumnProto.newBuilder();
    column.name = name;
    column.dataType = dataType;
    return column;
  }

	@Override
	public ColumnProto getProto() {
	  if(!viaProto) {
      mergeLocalToBuilder();
      proto = builder.build();
      viaProto = true;
    }
	  
	  return proto;
	}
	
	private void setModified() {
	  if (viaProto && builder == null) {
	    builder = ColumnProto.newBuilder(proto);
	  }
	  viaProto = false;
	}
	
	private void mergeLocalToBuilder() {
	  if (builder == null) {
	    builder = ColumnProto.newBuilder(proto);
	  }
		if (this.name != null) {
			builder.setColumnName(this.name);			
		}
		if (this.dataType != null) {
			builder.setDataType(this.dataType);
		}
	}
	
	public String toString() {
	  return getQualifiedName() +" (" + getDataType().getType() +")";
	}
	
	public String toJSON() {
		initFromProto();
		return GsonCreator.getInstance().toJson(this);
	}

	@Override
	public void initFromProto() {
		ColumnProtoOrBuilder p = viaProto ? proto : builder;
		if (this.name == null && p.hasColumnName()) {
			this.name = p.getColumnName();
		}
		if (this.dataType == null && p.hasDataType()) {
			this.dataType = p.getDataType();
		}
	}
}