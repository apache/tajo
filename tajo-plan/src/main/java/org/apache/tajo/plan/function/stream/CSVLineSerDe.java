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

package org.apache.tajo.plan.function.stream;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.storage.StorageConstants;
import org.apache.tajo.util.Bytes;

public class CSVLineSerDe extends TextLineSerDe {
  @Override
  public TextLineDeserializer createDeserializer(Schema schema, TableMeta meta, int[] targetColumnIndexes) {
    return new CSVLineDeserializer(schema, meta, targetColumnIndexes);
  }

  @Override
  public TextLineSerializer createSerializer(TableMeta meta) {
    return new CSVLineSerializer(meta);
  }

  public static byte[] getFieldDelimiter(TableMeta meta) {
    return StringEscapeUtils.unescapeJava(meta.getOption(StorageConstants.TEXT_DELIMITER,
        StorageConstants.DEFAULT_FIELD_DELIMITER)).getBytes(Bytes.UTF8_CHARSET);
  }
}
