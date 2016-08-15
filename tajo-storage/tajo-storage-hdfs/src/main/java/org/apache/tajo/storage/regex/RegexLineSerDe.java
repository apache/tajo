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

package org.apache.tajo.storage.regex;

import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.storage.text.TextLineDeserializer;
import org.apache.tajo.storage.text.TextLineSerDe;
import org.apache.tajo.storage.text.TextLineSerializer;


/**
 * This is an implementation copied from hive RegexSerDe
 *
 * RegexSerDe uses regular expression (regex) to serialize/deserialize.
 *
 * It can deserialize the data using regex and extracts groups as columns. It
 * can also serialize the tuple using a format string.
 *
 * In deserialization stage, if a row does not match the regex, then all columns
 * in the row will be NULL. If a row matches the regex but has less than
 * expected groups, the missing groups will be NULL. If a row matches the regex
 * but has more than expected groups, the additional groups are just ignored.
 *
 * In serialization stage, it uses java string formatter to format the columns
 * into a row. If the output type of the column in a query is not a string, it
 * will be automatically converted to String by tajo.
 *
 * For the format of the format String, please refer to
 * {@link http://java.sun.com/j2se/1.5.0/docs/api/java/util/Formatter.html#syntax}
 */
public class RegexLineSerDe extends TextLineSerDe {

  @Override
  public TextLineDeserializer createDeserializer(Schema schema, TableMeta meta, Column [] projected) {
    return new RegexLineDeserializer(schema, meta, projected);
  }

  @Override
  public TextLineSerializer createSerializer(Schema schema, TableMeta meta) {
    return new RegexLineSerializer(schema, meta);
  }
}
