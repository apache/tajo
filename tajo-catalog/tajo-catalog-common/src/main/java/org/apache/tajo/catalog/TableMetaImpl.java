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
import org.apache.tajo.catalog.json.GsonCreator;
import org.apache.tajo.catalog.proto.CatalogProtos.StoreType;
import org.apache.tajo.catalog.proto.CatalogProtos.TableProto;
import org.apache.tajo.catalog.proto.CatalogProtos.TableProtoOrBuilder;
import org.apache.tajo.catalog.statistics.TableStat;

import java.util.Iterator;
import java.util.Map.Entry;

public class TableMetaImpl implements TableMeta {
	protected TableProto proto = TableProto.getDefaultInstance();
	protected TableProto.Builder builder = null;
	protected boolean viaProto = false;	
	
	@Expose
	protected Schema schema;
	@Expose
	protected StoreType storeType;
	@Expose
	protected Options options;
	@Expose
	protected TableStat stat;
	
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
		this.proto = proto;
		this.viaProto = true;
	}
	
	public void setStorageType(StoreType storeType) {
    setModified();
    this.storeType = storeType;
  }	
	
	public StoreType getStoreType() {
	  TableProtoOrBuilder p = viaProto ? proto : builder;
		
		if(storeType != null) {
			return this.storeType;
		}
		if(!p.hasStoreType()) {
			return null;
		}
		this.storeType = p.getStoreType();
		
		return this.storeType;		
	}
	
  public void setSchema(Schema schema) {
    setModified();
    this.schema = schema;
  }
	
	public Schema getSchema() {
		TableProtoOrBuilder p = viaProto ? proto : builder;
		
		if(schema != null) {
			return this.schema;
		}
		if(!proto.hasSchema()) {
		  return null;
		}
		this.schema = new Schema(p.getSchema());
		
		return this.schema;
	}
	
  public void setOptions(Options options) {
    setModified();
    this.options = options;
  }

  private Options initOptions() {
    TableProtoOrBuilder p = viaProto ? proto : builder;
    if(this.options != null) {
      return this.options;
    }
    if(!p.hasParams()) {
      return null;
    }
    this.options = new Options(p.getParams());
    
    return this.options;
  }  

  @Override
  public void putOption(String key, String val) {
    setModified();
    initOptions().put(key, val);
  }
  

  @Override
  public String getOption(String key) {    
    return initOptions().get(key);
  }

  @Override
  public String getOption(String key, String defaultValue) {
    return initOptions().get(key, defaultValue);
  }
  
  @Override
  public Iterator<Entry<String,String>> getOptions() {    
    return initOptions().getAllKeyValus();
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
	  initFromProto();
	  TableMetaImpl meta = (TableMetaImpl) super.clone();	  
	  meta.proto = null;
    meta.viaProto = false;
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
	  if(!viaProto) {
      mergeLocalToBuilder();
      proto = builder.build();
      viaProto = true;  
    }
		return proto;
	}

  private void setModified() {
    if (viaProto || builder == null) {
      builder = TableProto.newBuilder(proto);
    }
    this.viaProto = false;
  }
	
	private void mergeLocalToBuilder() {
    if (this.builder == null) {      
      this.builder = TableProto.newBuilder(proto);
    }
	  
	  if (this.schema != null) {
	    builder.setSchema(this.schema.getProto());
	  }
	  
	  if (this.stat != null) {
	    builder.setStat(this.stat.getProto());
	  }

	  if (this.storeType != null) {
      builder.setStoreType(storeType);
    }

		if (this.options != null) {
		  builder.setParams(options.getProto());
		}
	}
	
  ////////////////////////////////////////////////////////////////////////
  // For Json
  ////////////////////////////////////////////////////////////////////////	
	private void mergeProtoToLocal() {
		TableProtoOrBuilder p = viaProto ? proto : builder;
		if (schema == null) {
			schema = new Schema(p.getSchema());
		}
		if (p.hasStat() && stat == null) {
		  stat = new TableStat(p.getStat());
		}
		if (storeType == null && p.hasStoreType()) {
			storeType = p.getStoreType();
		}
		if (options == null && p.hasParams()) {
			options = new Options(p.getParams());
		}
	}
	
	public void initFromProto() {
		mergeProtoToLocal();
    schema.initFromProto();
    if (stat != null) {
      stat.initFromProto();
    }
	}
	
	public String toJSON() {
		initFromProto();
		Gson gson = GsonCreator.getInstance();
		return gson.toJson(this, TableMeta.class);
	}

  @Override
  public void setStat(TableStat stat) {
    setModified();
    this.stat = stat;
  }

  @Override
  public TableStat getStat() {
    TableProtoOrBuilder p = viaProto ? proto : builder;
    if (stat != null) {
      return stat;
    }
    if (!p.hasStat()) {
      return null;
    }
    stat = new TableStat(p.getStat());
    
    return this.stat;
  }
}
