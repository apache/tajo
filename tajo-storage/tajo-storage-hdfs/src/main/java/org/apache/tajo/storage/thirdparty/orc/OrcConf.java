//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.

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

package org.apache.tajo.storage.thirdparty.orc;

import org.apache.hadoop.conf.Configuration;

// All configs in this class also appear in HiveConf, so any changes here should also be made there
// This is because only HiveConf can provide type checking through the CLI, and Presto depends on
// open source Hive, and so won't work with any variables not in open source HiveConf
public class OrcConf {

  public enum ConfVars {
    HIVE_ORC_DICTIONARY_KEY_SIZE_THRESHOLD("hive.exec.orc.dictionary.key.size.threshold", 0.8f),

    // Maximum fraction of heap that can be used by ORC file writers
    HIVE_ORC_FILE_MEMORY_POOL("hive.exec.orc.memory.pool", 0.5f), // 50%
    HIVE_ORC_FILE_MIN_MEMORY_ALLOCATION("hive.exec.orc.min.mem.allocation", 4194304L), // 4 Mb
    HIVE_ORC_FILE_ENABLE_LOW_MEMORY_MODE("hive.exec.orc.low.memory", false),
    HIVE_ORC_ROW_BUFFER_SIZE("hive.exec.orc.row.buffer.size", 100),

    HIVE_ORC_EAGER_HDFS_READ("hive.exec.orc.eager.hdfs.read", true),
    HIVE_ORC_EAGER_HDFS_READ_BYTES("hive.exec.orc.eager.hdfs.read.bytes", 193986560), // 185 Mb

    HIVE_ORC_ROW_INDEX_STRIDE_DICTIONARY_CHECK("hive.orc.row.index.stride.dictionary.check", true),
    HIVE_ORC_DEFAULT_STRIPE_SIZE("hive.exec.orc.default.stripe.size", 64L * 1024 * 1024),
    HIVE_ORC_DEFAULT_BLOCK_SIZE("hive.exec.orc.default.block.size", 256L * 1024 * 1024),
    HIVE_ORC_DEFAULT_ROW_INDEX_STRIDE("hive.exec.orc.default.row.index.stride", 10000),
    HIVE_ORC_DEFAULT_BUFFER_SIZE("hive.exec.orc.default.buffer.size", 256 * 1024),
    HIVE_ORC_DEFAULT_BLOCK_PADDING("hive.exec.orc.default.block.padding", true),
    HIVE_ORC_DEFAULT_COMPRESS("hive.exec.orc.default.compress", "ZLIB"),
    HIVE_ORC_WRITE_FORMAT("hive.exec.orc.write.format", null), // 0.11 or 0.12
    HIVE_ORC_ENCODING_STRATEGY("hive.exec.orc.encoding.strategy", "SPEED"),
    HIVE_ORC_COMPRESSION_STRATEGY("hive.exec.orc.compression.strategy", "SPEED"),
    HIVE_ORC_BLOCK_PADDING_TOLERANCE("hive.exec.orc.block.padding.tolerance", 0.05f),

    ;

    public final String varname;
    public final String defaultVal;
    public final int defaultIntVal;
    public final long defaultLongVal;
    public final float defaultFloatVal;
    public final boolean defaultBoolVal;


    ConfVars(String varname, String defaultVal) {
      this.varname = varname;
      this.defaultVal = defaultVal;
      this.defaultIntVal = -1;
      this.defaultLongVal = -1;
      this.defaultFloatVal = -1;
      this.defaultBoolVal = false;
    }

    ConfVars(String varname, int defaultIntVal) {
      this.varname = varname;
      this.defaultVal = Integer.toString(defaultIntVal);
      this.defaultIntVal = defaultIntVal;
      this.defaultLongVal = -1;
      this.defaultFloatVal = -1;
      this.defaultBoolVal = false;
    }

    ConfVars(String varname, long defaultLongVal) {
      this.varname = varname;
      this.defaultVal = Long.toString(defaultLongVal);
      this.defaultIntVal = -1;
      this.defaultLongVal = defaultLongVal;
      this.defaultFloatVal = -1;
      this.defaultBoolVal = false;
    }

    ConfVars(String varname, float defaultFloatVal) {
      this.varname = varname;
      this.defaultVal = Float.toString(defaultFloatVal);
      this.defaultIntVal = -1;
      this.defaultLongVal = -1;
      this.defaultFloatVal = defaultFloatVal;
      this.defaultBoolVal = false;
    }

    ConfVars(String varname, boolean defaultBoolVal) {
      this.varname = varname;
      this.defaultVal = Boolean.toString(defaultBoolVal);
      this.defaultIntVal = -1;
      this.defaultLongVal = -1;
      this.defaultFloatVal = -1;
      this.defaultBoolVal = defaultBoolVal;
    }
  }

  public static int getIntVar(Configuration conf, ConfVars var) {
    return conf.getInt(var.varname, var.defaultIntVal);
  }

  public static void setIntVar(Configuration conf, ConfVars var, int val) {
    conf.setInt(var.varname, val);
  }

  public static long getLongVar(Configuration conf, ConfVars var) {
    return conf.getLong(var.varname, var.defaultLongVal);
  }

  public static void setLongVar(Configuration conf, ConfVars var, long val) {
    conf.setLong(var.varname, val);
  }

  public static float getFloatVar(Configuration conf, ConfVars var) {
    return conf.getFloat(var.varname, var.defaultFloatVal);
  }

  public static void setFloatVar(Configuration conf, ConfVars var, float val) {
    conf.setFloat(var.varname, val);
  }

  public static boolean getBoolVar(Configuration conf, ConfVars var) {
    return conf.getBoolean(var.varname, var.defaultBoolVal);
  }

  public static void setBoolVar(Configuration conf, ConfVars var, boolean val) {
    conf.setBoolean(var.varname, val);
  }

  public static String getVar(Configuration conf, ConfVars var) {
    return conf.get(var.varname, var.defaultVal);
  }

  public static void setVar(Configuration conf, ConfVars var, String val) {
    conf.set(var.varname, val);
  }
}
