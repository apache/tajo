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

package org.apache.tajo.pullserver;

public class PullServerConstants {

  /**
   * Pull server query parameters
   */
  public enum Param {
    // Common params
    REQUEST_TYPE("rtype"),  // can be one of 'm' for meta and 'c' for chunk.
    SHUFFLE_TYPE("stype"),  // can be one of 'r', 'h', and 's'.
    QUERY_ID("qid"),
    EB_ID("sid"),
    PART_ID("p"),
    TASK_ID("ta"),
    OFFSET("offset"),
    LENGTH("length"),

    // Range shuffle params
    START("start"),
    END("end"),
    FINAL("final");

    private String key;

    Param(String key) {
      this.key = key;
    }

    public String key() {
      return key;
    }
  }

  // Request types ----------------------------------------------------------

  public static final String CHUNK_REQUEST_PARAM_STRING = "c";
  public static final String META_REQUEST_PARAM_STRING = "m";

  // Shuffle types ----------------------------------------------------------

  public static final String RANGE_SHUFFLE_PARAM_STRING = "r";
  public static final String HASH_SHUFFLE_PARAM_STRING = "h";
  public static final String SCATTERED_HASH_SHUFFLE_PARAM_STRING = "s";

  // HTTP header ------------------------------------------------------------

  public static final String CHUNK_LENGTH_HEADER_NAME = "c";

  // SSL configurations -----------------------------------------------------

  public static final int DEFAULT_SUFFLE_SSL_FILE_BUFFER_SIZE = 60 * 1024;

  public static final String SUFFLE_SSL_FILE_BUFFER_SIZE_KEY =
      "tajo.pullserver.ssl.file.buffer.size";

  // OS cache configurations ------------------------------------------------

  public static final String SHUFFLE_MANAGE_OS_CACHE = "tajo.pullserver.manage.os.cache";
  public static final boolean DEFAULT_SHUFFLE_MANAGE_OS_CACHE = true;

  // Prefetch configurations ------------------------------------------------

  public static final String SHUFFLE_READAHEAD_BYTES = "tajo.pullserver.readahead.bytes";
  public static final int DEFAULT_SHUFFLE_READAHEAD_BYTES = 4 * 1024 * 1024;

  // Yarn service ID --------------------------------------------------------

  public static final String PULLSERVER_SERVICEID = "tajo.pullserver";

  // Standalone pull server -------------------------------------------------
  public static final String PULLSERVER_STANDALONE_ENV_KEY = "TAJO_PULLSERVER_STANDALONE";

  public static final String PULLSERVER_SERVICE_NAME = "httpshuffle";
}
