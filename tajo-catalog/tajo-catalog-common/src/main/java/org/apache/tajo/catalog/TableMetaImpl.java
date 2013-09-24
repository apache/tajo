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
import org.apache.tajo.catalog.json.CatalogGsonHelper;
import org.apache.tajo.catalog.proto.CatalogProtos.StoreType;
import org.apache.tajo.catalog.proto.CatalogProtos.TableProto;
import org.apache.tajo.catalog.proto.CatalogProtos.TableProtoOrBuilder;
import org.apache.tajo.catalog.statistics.TableStat;
import org.apache.tajo.json.GsonObject;

import java.util.Map;

public class TableMetaImpl implements TableMeta, GsonObject {
	protected TableProto.Builder builder = null;
  private TableProto proto = TableProto.getDefaultInstance();
  private boolean viaProto = false;
	
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
    this.proto = proto;
    viaProto = true;
	}
	
	public void setStorageType(StoreType storeType) {
    maybeInitBuilder();
    this.storeType = storeType;
  }	
	
	public StoreType getStoreType() {
    TableProtoOrBuilder p = viaProto ? proto : builder;
    if (this.storeType != null) {
      return storeType;
    }
    if (!p.hasStoreType()) {
      return null;
    }
    this.storeType = p.getStoreType();
		return this.storeType;		
	}
	
  public void setSchema(Schema schema) {
    maybeInitBuilder();
    this.schema = schema;
  }
	
  public void setOptions(Options options) {
    maybeInitBuilder();
    this.options = options;
  }

  @Override
  public void putOption(String key, String val) {
    maybeInitBuilder();
    options.put(key, val);
  }

  public Schema getSchema() {
    TableProtoOrBuilder p = viaProto ? proto : builder;
    if (schema != null) {
      return this.schema;
    }
    if (!p.hasSchema()) {
      return null;
    }
    this.schema = new Schema(p.getSchema());
    return this.schema;
  }

  @Override
  public String getOption(String key) {
    TableProtoOrBuilder p = viaProto ? proto : builder;
    if (options != null) {
      return this.options.get(key);
    }
    if (!p.hasParams()) {
      return null;
    }
    this.options = new Options(p.getParams());
    return options.get(key);
  }

  @Override
  public String getOption(String key, String defaultValue) {
    TableProtoOrBuilder p = viaProto ? proto : builder;
    if (options != null) {
      return this.options.get(key, defaultValue);
    }
    if (!p.hasParams()) {
      return null;
    }
    this.options = new Options(p.getParams());
    return options.get(key, defaultValue);
  }
  
  @Override
  public Map<String,String> getOptions() {
    TableProtoOrBuilder p = viaProto ? proto : builder;
    if (options != null) {
      return this.options.getAllKeyValus();
    }
    if (!p.hasParams()) {
      return null;
    }
    this.options = new Options(p.getParams());
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
	  return Objects.hashCode(getSchema(), getStoreType());
	}
	
	@Override
	public Object clone() throws CloneNotSupportedException {
	  TableMetaImpl meta = (TableMetaImpl) super.clone();
    meta.builder = TableProto.newBuilder();
    meta.schema = (Schema) getSchema().clone();
    meta.storeType = getStoreType();
    meta.stat = (TableStat) (getStat() != null ? stat.clone() : null);
    meta.options = (Options) (getOptions() != null ? options.clone() : null);
    
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
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
	}

  @Override
	public String toJson() {
    mergeProtoToLocal();
		return CatalogGsonHelper.toJson(this, TableMeta.class);
	}

  public void mergeProtoToLocal() {
    getSchema();
    getStoreType();
    getOptions();
    getStat();
  }

  @Override
  public void setStat(TableStat stat) {
    maybeInitBuilder();
    this.stat = stat;
  }

  @Override
  public TableStat getStat() {
    TableProtoOrBuilder p = viaProto ? proto : builder;
    if (this.stat != null) {
      return this.stat;
    }
    if (!p.hasStat()) {
      return null;
    }
    this.stat = new TableStat(p.getStat());
    return this.stat;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = TableProto.newBuilder(proto);
    }
    viaProto = true;
  }

  private void mergeLocalToBuilder() {
    if (schema != null) {
      builder.setSchema(schema.getProto());
    }
    if (storeType != null) {
      builder.setStoreType(storeType);
    }
    if (this.options != null) {
      builder.setParams(options.getProto());
    }
    if (this.stat != null) {
      builder.setStat(stat.getProto());
    }
  }

  private void mergeLocalToProto() {
    if(viaProto) {
      maybeInitBuilder();
    }
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }
}
