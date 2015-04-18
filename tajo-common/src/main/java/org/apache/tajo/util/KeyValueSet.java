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

package org.apache.tajo.util;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.gson.annotations.Expose;
import org.apache.tajo.common.ProtoObject;
import org.apache.tajo.json.CommonGsonHelper;
import org.apache.tajo.json.GsonObject;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import static org.apache.tajo.rpc.protocolrecords.PrimitiveProtos.KeyValueProto;
import static org.apache.tajo.rpc.protocolrecords.PrimitiveProtos.KeyValueSetProto;

public class KeyValueSet implements ProtoObject<KeyValueSetProto>, Cloneable, GsonObject {
  public static final String TRUE_STR = "true";
  public static final String FALSE_STR = "false";

  @Expose private Map<String,String> keyVals;
	
	public KeyValueSet() {
    keyVals = TUtil.newHashMap();
	}

  public KeyValueSet(Map<String, String> keyVals) {
    this();
    putAll(keyVals);
  }
	
	public KeyValueSet(KeyValueSetProto proto) {
    this.keyVals = TUtil.newHashMap();
    for(KeyValueProto keyval : proto.getKeyvalList()) {
      this.keyVals.put(keyval.getKey(), keyval.getValue());
    }
	}
	
	public KeyValueSet(KeyValueSet keyValueSet) {
	  this();
	  this.keyVals.putAll(keyValueSet.keyVals);
	}
	
	public static KeyValueSet create() {
	  return new KeyValueSet();
	}
	
	public static KeyValueSet create(KeyValueSet keyValueSet) {
    return new KeyValueSet(keyValueSet);
  }

  public int size() {
    return keyVals.size();
  }

  public void putAll(Map<String, String> keyValues) {
    if (keyValues != null) {
      this.keyVals.putAll(keyValues);
    }
  }

  public void putAll(KeyValueSet keyValueSet) {
    if (keyValueSet != null) {
      this.keyVals.putAll(keyValueSet.keyVals);
    }
  }

  public Map<String,String> getAllKeyValus() {
    return keyVals;
  }

  public boolean containsKey(String key) {
    return this.keyVals.containsKey(key);
  }

  public void set(String key, String val) {
    Preconditions.checkNotNull(key);
    Preconditions.checkNotNull(val);

    this.keyVals.put(key, val);
  }

  public String get(String key, String defaultVal) {
    if(keyVals.containsKey(key)) {
      return keyVals.get(key);
    } else if (defaultVal != null) {
      return defaultVal;
    } else {
      throw new IllegalArgumentException("No such config key: "  + key);
    }
  }

  public String get(String key) {
    return get(key, null);
  }

  public void setBool(String key, boolean val) {
    set(key, val ? TRUE_STR : FALSE_STR);
  }

  public boolean getBool(String key, Boolean defaultVal) {
    if (containsKey(key)) {
      String strVal = get(key, null);
      return strVal != null ? strVal.equalsIgnoreCase(TRUE_STR) : false;
    } else if (defaultVal != null) {
      return defaultVal;
    } else {
      return false;
    }
  }

  public boolean getBool(String key) {
    return getBool(key, null);
  }

  public void setInt(String key, int val) {
    set(key, String.valueOf(val));
  }

  public int getInt(String key, Integer defaultVal) {
    if (containsKey(key)) {
      String strVal = get(key, null);
      return Integer.parseInt(strVal);
    } else if (defaultVal != null) {
      return defaultVal;
    } else {
      throw new IllegalArgumentException("No such a config key: "  + key);
    }
  }

  public int getInt(String key) {
    return getInt(key, null);
  }

  public void setLong(String key, long val) {
    set(key, String.valueOf(val));
  }

  public long getLong(String key, Long defaultVal) {
    if (containsKey(key)) {
      String strVal = get(key, null);
      return Long.parseLong(strVal);
    } else if (defaultVal != null) {
      return defaultVal;
    } else {
      throw new IllegalArgumentException("No such a config key: "  + key);
    }
  }

  public long getLong(String key) {
    return getLong(key, null);
  }

  public void setFloat(String key, float val) {
    set(key, String.valueOf(val));
  }

  public float getFloat(String key, Float defaultVal) {
    if (containsKey(key)) {
      String strVal = get(key, null);
      try {
        if (Float.MAX_VALUE < Double.parseDouble(strVal)) {
          throw new IllegalStateException("Parsed value is overflow in float type");
        }
        return Float.parseFloat(strVal);
      } catch (NumberFormatException nfe) {
        throw new IllegalArgumentException("No such a config key: "  + key);
      }
    } else if (defaultVal != null) {
      return defaultVal.floatValue();
    } else {
      throw new IllegalArgumentException("No such a config key: "  + key);
    }
  }

  public float getFloat(String key) {
    return getFloat(key, null);
  }
	
	public String remove(String key) {
		return keyVals.remove(key);
	}

  @Override
  public int hashCode() {
    return Objects.hashCode(keyVals);

  }

  @Override
	public boolean equals(Object object) {
		if(object instanceof KeyValueSet) {
			KeyValueSet other = (KeyValueSet)object;
			for(Entry<String, String> entry : other.keyVals.entrySet()) {
				if(!keyVals.containsKey(entry.getKey()) || !keyVals.get(entry.getKey()).equals(entry.getValue()))
					return false;
			}
			return true;
		}
		
		return false;
	}
	
	@Override
  public Object clone() throws CloneNotSupportedException {    
    KeyValueSet keyValueSet = (KeyValueSet) super.clone();
    keyValueSet.keyVals = keyVals != null ? new HashMap<String, String>(keyVals) : null;
    return keyValueSet;
	}
	
	@Override
	public KeyValueSetProto getProto() {
    KeyValueSetProto.Builder builder = KeyValueSetProto.newBuilder();

    KeyValueProto.Builder kvBuilder = KeyValueProto.newBuilder();
    if(this.keyVals != null) {
      for(Entry<String,String> kv : keyVals.entrySet()) {
        kvBuilder.setKey(kv.getKey());
        kvBuilder.setValue(kv.getValue());
        builder.addKeyval(kvBuilder.build());

        kvBuilder.clear();
      }
    }
    return builder.build();
	}
  
  public String toJson() {
    return CommonGsonHelper.toJson(this, KeyValueSet.class);
  }
}
