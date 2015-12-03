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
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.catalog.proto.CatalogProtos.TableProto;
import org.apache.tajo.common.ProtoObject;
import org.apache.tajo.json.GsonObject;
import org.apache.tajo.util.KeyValueSet;

import java.util.Map;

/**
 * It contains all information for scanning a fragmented table
 */
public class TableMeta implements ProtoObject<CatalogProtos.TableProto>, GsonObject, Cloneable {
	@Expose protected String dataFormat;
	@Expose protected KeyValueSet propertySet;
	
	public TableMeta(String dataFormat, KeyValueSet propertySet) {
    this.dataFormat = dataFormat;
    this.propertySet = new KeyValueSet(propertySet);
  }
	
	public TableMeta(TableProto proto) {
    this.dataFormat = proto.getDataFormat();
    this.propertySet = new KeyValueSet(proto.getParams());
	}
	
	public String getDataFormat() {
		return this.dataFormat;
	}
	
  public void setPropertySet(KeyValueSet options) {
    this.propertySet = options;
  }

  public KeyValueSet getPropertySet() {
    return propertySet;
  }

  public void putProperty(String key, String val) {
    propertySet.set(key, val);
  }

  public boolean containsProperty(String key) {
    return propertySet.containsKey(key);
  }

  public String getProperty(String key) {
    return propertySet.get(key);
  }

  public String getProperty(String key, String defaultValue) {
    return propertySet.get(key, defaultValue);
  }

  public Map<String,String> toMap() {
    return getPropertySet().getAllKeyValus();
  }
	
	public boolean equals(Object object) {
		if(object instanceof TableMeta) {
			TableMeta other = (TableMeta) object;

			boolean eq = this.getDataFormat().equals(other.getDataFormat());
			eq = eq && this.getPropertySet().equals(other.getPropertySet());
			return eq;
		}
		
		return false;		
	}
	
	public int hashCode() {
	  return Objects.hashCode(getDataFormat(), getPropertySet());
	}
	
	@Override
	public Object clone() throws CloneNotSupportedException {
	  TableMeta meta = (TableMeta) super.clone();
    meta.dataFormat = getDataFormat();
    meta.propertySet = (KeyValueSet) (toMap() != null ? propertySet.clone() : null);
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
	public TableProto getProto() {
    TableProto.Builder builder = TableProto.newBuilder();
    builder.setDataFormat(dataFormat);
    builder.setParams(propertySet.getProto());
    return builder.build();
	}

  @Override
	public String toJson() {
    mergeProtoToLocal();
		return CatalogGsonHelper.toJson(this, TableMeta.class);
	}

  public void mergeProtoToLocal() {
    getDataFormat();
    toMap();
  }
}
