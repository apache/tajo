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
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.Expose;
import org.apache.tajo.json.GsonObject;
import org.apache.tajo.catalog.json.CatalogGsonHelper;
import org.apache.tajo.catalog.proto.CatalogProtos.StoreType;
import org.apache.tajo.catalog.proto.CatalogProtos.TableProto;
import org.apache.tajo.catalog.statistics.TableStat;

import java.util.Iterator;
import java.util.Map.Entry;

public class TableMetaImpl implements TableMeta, GsonObject {
	protected TableProto.Builder builder = null;
	
	@Expose protected Schema schema;
	@Expose protected StoreType storeType;
	@Expose protected Options options;
	@Expose	protected TableStat stat;
	
	private TableMetaImpl() {
	  builder = TableProto.newBuilder();
	}
	
	public TableMetaImpl(Schema schema, StoreType type, Options options) {
	  this();
	  this.schema = schema;
    this.storeType = type;
    this.options = new Options(options);
  }
	
	public TableMetaImpl(Schema schema, StoreType type, Options options,
	    TableStat stat) {
    this();
    this.schema = schema;
    this.storeType = type;
    this.options = new Options(options);
    this.stat = stat;
  }
	
	public TableMetaImpl(TableProto proto) {
    this();
    schema = new Schema(proto.getSchema());
    storeType = proto.getStoreType();
    options = new Options(proto.getParams());

    if (proto.hasStat() && stat == null) {
      stat = new TableStat(proto.getStat());
    }
	}
	
	public void setStorageType(StoreType storeType) {
    this.storeType = storeType;
  }	
	
	public StoreType getStoreType() {
		return this.storeType;		
	}
	
  public void setSchema(Schema schema) {
    this.schema = schema;
  }
	
	public Schema getSchema() {
		return this.schema;
	}
	
  public void setOptions(Options options) {
    this.options = options;
  }

  @Override
  public void putOption(String key, String val) {
    options.put(key, val);
  }
  

  @Override
  public String getOption(String key) {    
    return options.get(key);
  }

  @Override
  public String getOption(String key, String defaultValue) {
    return options.get(key, defaultValue);
  }
  
  @Override
  public Iterator<Entry<String,String>> getOptions() {    
    return options.getAllKeyValus();
  }
	
	public boolean equals(Object object) {
		if(object instanceof TableMetaImpl) {
			TableMetaImpl other = (TableMetaImpl) object;
			
			return this.getProto().equals(other.getProto());
		}
		
		return false;		
	}
	
	public int hashCode() {
	  return Objects.hashCode(getSchema(), storeType);
	}
	
	@Override
	public Object clone() throws CloneNotSupportedException {
	  TableMetaImpl meta = (TableMetaImpl) super.clone();
    meta.builder = TableProto.newBuilder();
    meta.schema = (Schema) schema.clone();
    meta.storeType = storeType;
    meta.stat = (TableStat) (stat != null ? stat.clone() : null);
    meta.options = (Options) (options != null ? options.clone() : null);
    
    return meta;
	}
	
	public String toString() {
	  Gson gson = new GsonBuilder().setPrettyPrinting().
        excludeFieldsWithoutExposeAnnotation().create();
	  return gson.toJson(this);
  }
	
	////////////////////////////////////////////////////////////////////////
	// ProtoObject
	////////////////////////////////////////////////////////////////////////
	@Override
	public TableProto getProto() {
    if (builder == null) {
      builder = TableProto.newBuilder();
    }
    builder.setSchema(this.schema.getProto());
    builder.setStoreType(storeType);
    builder.setParams(options.getProto());

    if (this.stat != null) {
      builder.setStat(this.stat.getProto());
    }
    return builder.build();
	}

  @Override
	public String toJson() {
		return CatalogGsonHelper.toJson(this, TableMeta.class);
	}

  @Override
  public void setStat(TableStat stat) {
    this.stat = stat;
  }

  @Override
  public TableStat getStat() {
    return this.stat;
  }
}
