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

import io.netty.buffer.ByteBuf;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.storage.Tuple;

import java.io.IOException;

/**
 * Reads a text line and fills a Tuple with values
 */
public abstract class TextLineDeserializer {
  protected final Schema schema;
  protected final TableMeta meta;
  protected final int[] targetColumnIndexes;

  public TextLineDeserializer(Schema schema, TableMeta meta, int[] targetColumnIndexes) {
    this.schema = schema;
    this.meta = meta;
    this.targetColumnIndexes = targetColumnIndexes;
  }

  /**
   * Initialize SerDe
   */
  public abstract void init();

  /**
   * It fills a tuple with a read fields in a given line.
   *
   * @param buf Read line
   * @param output Tuple to be filled with read fields
   * @throws IOException
   */
  public abstract void deserialize(final ByteBuf buf, Tuple output) throws IOException, TextLineParsingError;

  /**
   * Release external resources
   */
  public abstract void release();
}
