/*
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

package org.apache.tajo;

import java.util.Arrays;

import com.google.common.base.Preconditions;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.util.KeyValueSet;

import static org.apache.tajo.ConfigKey.ConfigType;
import static org.apache.tajo.rpc.protocolrecords.PrimitiveProtos.KeyValueSetProto;

/**
 * OverridableConf provides a consolidated config system. Tajo basically uses TajoConf, which is a extended class of
 * Hadoop's Configuration system, However, TajoConf is only used for sharing static system configs, such as binding
 * address of master and workers, system directories, other system parameters.
 *
 * For modifiable or instant configs, we use OverridableConf, which is a set of key-value pairs.
 * OverridableConf provides more strong-typed way to set configs and its behavior is more clear than Configuration
 * system.
 *
 * By default, OverridableConf recognizes following config types.
 *
 * <ul>
 *    <li>System Config - it comes from Hadoop's Configuration class. by tajo-site, catalog-site,
 *    catalog-default and TajoConf.</li>
 *    <li>Session variables - they are instantly configured by users.
 *    Each client session has it own set of session variables.</li>
 * </ul>
 *
 * System configs and session variables can set the same config in the same time. System configs are usually used to set
 * default configs, and session variables is user-specified configs. So, session variables can override system configs.
 */
public class OverridableConf extends KeyValueSet {
  private static final Log LOG = LogFactory.getLog(OverridableConf.class);
  private ConfigType [] configTypes;
  protected TajoConf conf;

  public OverridableConf(final TajoConf conf, ConfigType...configTypes) {
    this.conf = conf;
    this.configTypes = configTypes;
  }

  public OverridableConf(final TajoConf conf, KeyValueSetProto proto, ConfigType...configTypes) {
    super(proto);
    this.conf = conf;
    this.configTypes = configTypes;
  }

  public void setConf(TajoConf conf) {
    this.conf = conf;
  }

  public TajoConf getConf() {
    return conf;
  }

  public void setBool(ConfigKey key, boolean val) {
    setBool(key.keyname(), val);
  }

  public boolean getBool(ConfigKey key, Boolean defaultVal) {
    assertRegisteredEnum(key);

    if (key.type() != ConfigType.SESSION && key.type() != ConfigType.SYSTEM) {
      return getBool(key.keyname(), defaultVal);
    } else {
      switch (key.type()) {
      case QUERY:
        return getBool(key.keyname());
      case SESSION:
        return getBool(key.keyname(), conf.getBoolVar(((SessionVars) key).getConfVars()));
      case SYSTEM:
        return conf.getBoolVar((TajoConf.ConfVars) key);
      default:
        throw new IllegalStateException("key does not belong to Session and System config sets");
      }
    }
  }

  @Override
  public boolean getBool(ConfigKey key) {
    return getBool(key, null);
  }

  public void setInt(ConfigKey key, int val) {
    setInt(key.keyname(), val);
  }

  public int getInt(ConfigKey key, Integer defaultVal) {
    assertRegisteredEnum(key);

    if (key.type() != ConfigType.SESSION && key.type() != ConfigType.SYSTEM) {
      return getInt(key.keyname(), defaultVal);
    } else {
      switch (key.type()) {
      case SESSION:
        return getInt(key.keyname(), conf.getIntVar(((SessionVars) key).getConfVars()));
      case SYSTEM:
        return conf.getIntVar((TajoConf.ConfVars) key);
      default:
        throw new IllegalStateException("key does not belong to Session and System config sets");
      }
    }
  }

  @Override
  public int getInt(ConfigKey key) {
    return getInt(key, null);
  }

  public void setLong(ConfigKey key, long val) {
    setLong(key.keyname(), val);
  }

  public long getLong(ConfigKey key, Long defaultVal) {
    assertRegisteredEnum(key);

    if (key.type() != ConfigType.SESSION && key.type() != ConfigType.SYSTEM) {
      return getLong(key.keyname(), defaultVal);
    } else {
      switch (key.type()) {
      case SESSION:
        return getLong(key.keyname(), conf.getLongVar(((SessionVars) key).getConfVars()));
      case SYSTEM:
        return conf.getLongVar((TajoConf.ConfVars) key);
      default:
        throw new IllegalStateException("key does not belong to Session and System config sets");
      }
    }
  }

  @Override
  public long getLong(ConfigKey key) {
    return getLong(key, null);
  }

  public void setFloat(ConfigKey key, float val) {
    setFloat(key.keyname(), val);
  }

  public float getFloat(ConfigKey key, Float defaultVal) {
    assertRegisteredEnum(key);

    if (key.type() != ConfigType.SESSION && key.type() != ConfigType.SYSTEM) {
      return getFloat(key.keyname(), defaultVal);
    } else {
      switch (key.type()) {
      case SESSION:
        return getFloat(key.keyname(), conf.getFloatVar(((SessionVars) key).getConfVars()));
      case SYSTEM:
        return conf.getFloatVar((TajoConf.ConfVars) key);
      default:
        throw new IllegalStateException("key does not belong to Session and System config sets");
      }
    }
  }

  @Override
  public float getFloat(ConfigKey key) {
    return getLong(key, null);
  }

  public void put(ConfigKey key, String val) {
    set(key.keyname(), val);
  }

  private void assertRegisteredEnum(ConfigKey key) {
    boolean registered = false;

    if (configTypes != null) {
      for (ConfigType c : configTypes) {
        registered = key.type() == c;
      }
    }

    // default permitted keys
    registered |= key.type() == ConfigType.SESSION || key.type() != ConfigType.SYSTEM;

    Preconditions.checkArgument(registered, key.keyname() + " (" + key.type() + ") is not allowed in " +
      getClass().getSimpleName());
  }

  public String get(ConfigKey key, String defaultVal) {
    assertRegisteredEnum(key);

    if (key.type() != ConfigType.SESSION && key.type() != ConfigType.SYSTEM) {
      return get(key.keyname(), defaultVal);
    } else {
      switch (key.type()) {
      case SESSION:
        return get(key.keyname(), conf.getVar(((SessionVars) key).getConfVars()));
      case SYSTEM:
        return conf.getVar((TajoConf.ConfVars) key);
      default:
        throw new IllegalStateException("key does not belong to Session and System config sets");
      }
    }
  }

  public String get(ConfigKey key) {
    return get(key, null);
  }

  public Class<?> getClass(ConfigKey key) {
    assertRegisteredEnum(key);

    String className = getTrimmed(key);
    try {
      return Class.forName(className);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  public String getTrimmed(ConfigKey key) {
    String value = get(key);

    if (null == value) {
      return null;
    } else {
      return value.trim();
    }
  }

  public boolean containsKey(ConfigKey key) {
    return containsKey(key.keyname());
  }

  public boolean equalKey(ConfigKey key, String another) {
    if (containsKey(key)) {
      return get(key).equals(another);
    } else {
      return false;
    }
  }
  
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + ((conf == null) ? 0 : conf.hashCode());
    result = prime * result + Arrays.hashCode(configTypes);
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    return super.equals(obj);
  }
  
}
