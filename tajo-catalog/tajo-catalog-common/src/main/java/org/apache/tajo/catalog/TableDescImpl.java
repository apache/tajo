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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.Expose;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.catalog.json.CatalogGsonHelper;
import org.apache.tajo.catalog.proto.CatalogProtos.StoreType;
import org.apache.tajo.catalog.proto.CatalogProtos.TableDescProto;
import org.apache.tajo.common.ProtoObject;

public class TableDescImpl implements TableDesc, ProtoObject<TableDescProto>, Cloneable {
  protected TableDescProto.Builder builder = null;
  
	@Expose protected String tableId; // required
	@Expose protected Path uri; // required
	@Expose protected TableMeta meta; // required
  
	public TableDescImpl() {
		builder = TableDescProto.newBuilder();
	}
	
	public TableDescImpl(String tableId, TableMeta info, Path path) {
		this();
		// tajo deems all identifiers as lowcase characters
	  this.tableId = tableId.toLowerCase();
	  this.meta = info;
	  this.uri = path;	   
	}
	
	public TableDescImpl(String tableId, Schema schema, StoreType type, 
	    Options options, Path path) {
	  this(tableId, new TableMetaImpl(schema, type, options), path);
	}
	
	public TableDescImpl(TableDescProto proto) {
	  this(proto.getId(), new TableMetaImpl(proto.getMeta()), new Path(proto.getPath()));
	}
	
	public void setId(String tableId) {
	  // tajo deems all identifiers as lowcase characters
		this.tableId = tableId.toLowerCase();
	}
	
  public String getId() {
    return this.tableId;
  }
	
	public void setPath(Path uri) {
		this.uri = uri;
	}
	
  public Path getPath() {
    return this.uri;
  }
  
  @Override
  public void setMeta(TableMeta info) {
    this.meta = info;
  }
	
	public TableMeta getMeta() {
	  return this.meta;
	}
	
  public Schema getSchema() {
    return getMeta().getSchema();
  }
	
	public boolean equals(Object object) {
    if(object instanceof TableDescImpl) {
      TableDescImpl other = (TableDescImpl) object;
      
      return this.getProto().equals(other.getProto());
    }
    
    return false;   
  }
	
	public Object clone() throws CloneNotSupportedException {	  
	  TableDescImpl desc = (TableDescImpl) super.clone();
	  desc.builder = TableDescProto.newBuilder();
	  desc.tableId = tableId;
	  desc.uri = uri;
	  desc.meta = (TableMeta) meta.clone();
	  
	  return desc;
	}
	
	public String toString() {
	  Gson gson = new GsonBuilder().setPrettyPrinting().
	      excludeFieldsWithoutExposeAnnotation().create();
    return gson.toJson(this);
	}
	
	public String toJson() {
		return CatalogGsonHelper.toJson(this, TableDesc.class);
	}

  public TableDescProto getProto() {
    if (builder == null) {
      builder = TableDescProto.newBuilder();
    }
    if (this.tableId != null) {
      builder.setId(this.tableId);
    }
    if (this.uri != null) {
      builder.setPath(this.uri.toString());
    }
    if (this.meta != null) {
      builder.setMeta(meta.getProto());
    }
    return builder.build();
  }
}