/*
 * Copyright 2012 Database Lab., Korea Univ.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * 
 */
package tajo.catalog;

import com.google.common.collect.Maps;
import com.google.gson.annotations.Expose;
import tajo.catalog.json.GsonCreator;
import tajo.catalog.proto.CatalogProtos.KeyValueProto;
import tajo.catalog.proto.CatalogProtos.KeyValueSetProto;
import tajo.catalog.proto.CatalogProtos.KeyValueSetProtoOrBuilder;
import tajo.common.ProtoObject;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

/**
 * @author Hyunsik Choi
 *
 */
public class Options implements ProtoObject<KeyValueSetProto>, Cloneable {
	@Expose(serialize=false,deserialize=false)
	private KeyValueSetProto proto = KeyValueSetProto.getDefaultInstance();
	@Expose(serialize=false,deserialize=false)
	private KeyValueSetProto.Builder builder = null;
	@Expose(serialize=false,deserialize=false)
	private boolean viaProto = false;
	
	@Expose private Map<String,String> keyVals;
	
	public Options() {
		builder = KeyValueSetProto.newBuilder();
	}
	
	public Options(KeyValueSetProto proto) {
		this.proto = proto;
		viaProto = true;
	}
	
	public Options(Options options) {
	  this();
	  options.initFromProto();
	  this.keyVals = Maps.newHashMap(options.keyVals);
	}
	
	public static Options create() {
	  return new Options();
	}
	
	public static Options create(Options options) {
    return new Options(options);
  }
	
	public void put(String key, String val) {
		initOptions();
		setModified();
		this.keyVals.put(key, val);
	}
	
	public void putAll(Options options) {
	  initOptions();
	  setModified();
	  this.keyVals.putAll(options.keyVals);
	}
	
	public String get(String key) {
		initOptions();
		return this.keyVals.get(key);
	}
	
	public String get(String key, String defaultVal) {
	  initOptions();
	  if(keyVals.containsKey(key))
	    return keyVals.get(key);
	  else {
	    return defaultVal;
	  }
	}
	
	public Iterator<Entry<String,String>> getAllKeyValus() {
	  initOptions();
	  return keyVals.entrySet().iterator();
	}
	
	public String delete(String key) {
		initOptions();
		return keyVals.remove(key);
	}
	
	@Override
	public boolean equals(Object object) {
		if(object instanceof Options) {
			Options other = (Options)object;
			initOptions();
			other.initOptions();
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
    initFromProto();
    options.proto = null;
    options.viaProto = false;
    options.builder = KeyValueSetProto.newBuilder();
    options.keyVals = keyVals != null ? new HashMap<>(keyVals) :
      null;    
    return options;
	}
	
	@Override
	public KeyValueSetProto getProto() {
	  if(!viaProto) {
      mergeLocalToBuilder();
      proto = builder.build();
      viaProto = true;
    }	  
		return proto;
	}
	
	private void initOptions() {
		if (this.keyVals != null) {
			return;
		}
		KeyValueSetProtoOrBuilder p = viaProto ? proto : builder;
		this.keyVals = Maps.newHashMap();
		for(KeyValueProto keyval:p.getKeyvalList()) {
			this.keyVals.put(keyval.getKey(), keyval.getValue());
		}		
	}
	
	private void setModified() {
		if (viaProto || builder == null) {
			builder = KeyValueSetProto.newBuilder(proto);
		}
		viaProto = false;
	}
	
	private void mergeLocalToBuilder() {
		KeyValueProto.Builder kvBuilder = null;
		if(this.keyVals != null) {
			for(Entry<String,String> kv : keyVals.entrySet()) {
				kvBuilder = KeyValueProto.newBuilder();
				kvBuilder.setKey(kv.getKey());
				kvBuilder.setValue(kv.getValue());
				builder.addKeyval(kvBuilder.build());
			}
		}
	}

  @Override
  public void initFromProto() {
    initOptions();
  }
  
  public String toJSON() {
    initFromProto();
    return GsonCreator.getInstance().toJson(this, Options.class);
  }
}
