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

package org.apache.tajo.storage.text;

import io.netty.buffer.ByteBuf;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.tajo.BuiltinStorages;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.storage.BufferPool;
import org.apache.tajo.storage.StorageConstants;
import org.apache.tajo.util.Bytes;

/**
 * Pluggable Text Line SerDe class
 */
public abstract class TextLineSerDe {

  public TextLineSerDe() {
  }

  public abstract TextLineDeserializer createDeserializer(Schema schema, TableMeta meta, Column [] projected);

  public abstract TextLineSerializer createSerializer(Schema schema, TableMeta meta);

  public static ByteBuf getNullChars(TableMeta meta) {
    byte[] nullCharByteArray = getNullCharsAsBytes(meta);
    ByteBuf nullChars = BufferPool.directBuffer(nullCharByteArray.length, nullCharByteArray.length);
    nullChars.writeBytes(nullCharByteArray);

    return nullChars;
  }

  /**
   * Returns the bytes of null characters.
   * The default value is '\\N' as in Hive.
   *
   * @param meta table meta
   * @return a byte array of null characters
   */
  public static byte [] getNullCharsAsBytes(TableMeta meta) {
    return getNullCharsAsBytes(meta, StorageConstants.TEXT_NULL, NullDatum.DEFAULT_TEXT);
  }

  public static byte[] getNullCharsAsBytes(TableMeta meta, String key, String defaultVal) {
    byte [] nullChars;

    String nullCharacters = StringEscapeUtils.unescapeJava(meta.getProperty(key, defaultVal));
    if (StringUtils.isEmpty(nullCharacters)) {
      nullChars = NullDatum.get().asTextBytes();
    } else {
      nullChars = nullCharacters.getBytes(Bytes.UTF8_CHARSET);
    }

    return nullChars;
  }

}
