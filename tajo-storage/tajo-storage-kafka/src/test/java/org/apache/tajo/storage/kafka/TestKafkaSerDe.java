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

package org.apache.tajo.storage.kafka;

import static org.junit.Assert.assertTrue;

import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SchemaBuilder;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.storage.VTuple;
import org.apache.tajo.storage.text.TextLineDeserializer;
import org.apache.tajo.util.KeyValueSet;
import org.junit.Test;

import java.util.Map;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class TestKafkaSerDe {
  private static Schema schema;

  static {
    schema = SchemaBuilder.builder()
        .add("col1", Type.INT4)
        .add("col2", Type.TEXT)
        .add("col3", Type.FLOAT4)
        .build();
  }

  // Test for deserializer.
  @Test
  public void testDeserializer() throws Exception {
    TableMeta meta = CatalogUtil.newTableMeta("KAFKA", new TajoConf());
    TextLineDeserializer deserializer = KafkaSerializerDeserializer.getTextSerde(meta).createDeserializer(schema, meta,
        schema.toArray());
    deserializer.init();
    VTuple tuple = new VTuple(schema.size());
    ByteBuf buf = Unpooled.wrappedBuffer(TestConstants.test_data[0].getBytes());
    deserializer.deserialize(buf, tuple);
    assertTrue(tuple.getInt4(0) == 1);
    assertTrue(tuple.getText(1).equals("abc"));
    assertTrue(tuple.getFloat4(2) == 0.2f);
  }

  // Test for json deserializer.
  @Test
  public void testJsonDeserializer() throws Exception {
    TableMeta meta = CatalogUtil.newTableMeta("KAFKA", new TajoConf());
    Map<String, String> option = new java.util.HashMap<String, String>();
    option.put(KafkaStorageConstants.KAFKA_SERDE_CLASS, "org.apache.tajo.storage.json.JsonLineSerDe");
    meta.setPropertySet(new KeyValueSet(option));
    int[] targetColumnIndexes = new int[schema.size()];
    for (int i = 0; i < schema.size(); i++) {
      targetColumnIndexes[i] = i;
    }
    TextLineDeserializer deserializer = KafkaSerializerDeserializer.getTextSerde(meta).createDeserializer(schema, meta,
        schema.toArray());
    deserializer.init();
    VTuple tuple = new VTuple(schema.size());
    ByteBuf buf = Unpooled.wrappedBuffer(TestConstants.test_json_data.getBytes());
    deserializer.deserialize(buf, tuple);
    assertTrue(tuple.getInt4(0) == 1);
    assertTrue(tuple.getText(1).equals("abc"));
    assertTrue(tuple.getFloat4(2) == 0.2f);
  }
}
