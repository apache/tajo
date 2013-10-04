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
import org.apache.tajo.catalog.json.CatalogGsonHelper;
import org.apache.tajo.catalog.proto.CatalogProtos.KeyValueProto;
import org.apache.tajo.catalog.proto.CatalogProtos.KeyValueSetProto;
import org.apache.tajo.common.ProtoObject;
import org.apache.tajo.json.GsonObject;
import org.apache.tajo.util.TUtil;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class Options implements ProtoObject<KeyValueSetProto>, Cloneable, GsonObject {
	private KeyValueSetProto.Builder builder = KeyValueSetProto.newBuilder();
	
	@Expose private Map<String,String> keyVals;
	
	public Options() {
    keyVals = TUtil.newHashMap();
	}
	
	public Options(KeyValueSetProto proto) {
    this.keyVals = TUtil.newHashMap();
    for(KeyValueProto keyval : proto.getKeyvalList()) {
      this.keyVals.put(keyval.getKey(), keyval.getValue());
    }
	}
	
	public Options(Options options) {
	  this();
	  this.keyVals.putAll(options.keyVals);
	}
	
	public static Options create() {
	  return new Options();
	}
	
	public static Options create(Options options) {
    return new Options(options);
  }
	
	public void put(String key, String val) {
		this.keyVals.put(key, val);
	}

  public void putAll(Map<String, String> keyValues) {
    if (keyValues != null) {
      this.keyVals.putAll(keyValues);
    }
  }
	
	public void putAll(Options options) {
    if (options != null) {
	    this.keyVals.putAll(options.keyVals);
    }
	}
	
	public String get(String key) {
		return this.keyVals.get(key);
	}
	
	public String get(String key, String defaultVal) {
	  if(keyVals.containsKey(key))
	    return keyVals.get(key);
	  else {
	    return defaultVal;
	  }
	}
	
	public Map<String,String> getAllKeyValus() {
	  return keyVals;
	}
	
	public String delete(String key) {
		return keyVals.remove(key);
	}
	
	@Override
	public boolean equals(Object object) {
		if(object instanceof Options) {
			Options other = (Options)object;
			for(Entry<String, String> entry : other.keyVals.entrySet()) {
				if(!keyVals.get(entry.getKey()).equals(entry.getValue()))
					return false;
			}
			return true;
		}
		
		return false;
	}
	
	@Override
  public Object clone() throws CloneNotSupportedException {    
    Options options = (Options) super.clone();
    options.builder = KeyValueSetProto.newBuilder();
    options.keyVals = keyVals != null ? new HashMap<String, String>(keyVals) : null;
    return options;
	}
	
	@Override
	public KeyValueSetProto getProto() {
    if (builder == null) {
      builder = KeyValueSetProto.newBuilder();
    } else {
      builder.clear();
    }

    KeyValueProto.Builder kvBuilder;
    if(this.keyVals != null) {
      for(Entry<String,String> kv : keyVals.entrySet()) {
        kvBuilder = KeyValueProto.newBuilder();
        kvBuilder.setKey(kv.getKey());

        kvBuilder.setValue(kv.getValue());
        builder.addKeyval(kvBuilder.build());
      }
    }
    return builder.build();
	}
  
  public String toJson() {
    return CatalogGsonHelper.toJson(this, Options.class);
  }
}
