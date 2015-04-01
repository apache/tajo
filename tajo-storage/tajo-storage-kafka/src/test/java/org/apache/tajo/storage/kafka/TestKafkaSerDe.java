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
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.util.Map;

import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.storage.VTuple;
import org.apache.tajo.storage.kafka.serDe.KafkaSerializerDeserializer;
import org.apache.tajo.storage.text.TextLineDeserializer;
import org.apache.tajo.util.KeyValueSet;
import org.junit.Test;

public class TestKafkaSerDe {
  private static Schema schema = new Schema();

  static {
    schema.addColumn("col1", Type.INT4);
    schema.addColumn("col2", Type.TEXT);
    schema.addColumn("col3", Type.FLOAT4);
  }

  // Test for deserializer.
  @Test
  public void testDeserializer() throws Exception {
    TableMeta meta = CatalogUtil.newTableMeta(CatalogProtos.StoreType.CSV);
    int[] targetColumnIndexes = new int[schema.size()];
    for (int i = 0; i < schema.size(); i++) {
      targetColumnIndexes[i] = i;
    }
    TextLineDeserializer deserializer = KafkaSerializerDeserializer.getTextSerde(meta).createDeserializer(schema, meta, targetColumnIndexes);
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
    TableMeta meta = CatalogUtil.newTableMeta(CatalogProtos.StoreType.CSV);
    Map<String,String> option = new java.util.HashMap<String, String>();
    option.put(KafkaStorageConstants.KAFKA_SERDE_CLASS, "org.apache.tajo.storage.json.JsonLineSerDe");
    meta.setOptions(new KeyValueSet(option));
    int[] targetColumnIndexes = new int[schema.size()];
    for (int i = 0; i < schema.size(); i++) {
      targetColumnIndexes[i] = i;
    }
    TextLineDeserializer deserializer = KafkaSerializerDeserializer.getTextSerde(meta).createDeserializer(schema, meta, targetColumnIndexes);
    deserializer.init();
    VTuple tuple = new VTuple(schema.size());
    ByteBuf buf = Unpooled.wrappedBuffer(TestConstants.test_json_data.getBytes());
    deserializer.deserialize(buf, tuple);
    assertTrue(tuple.getInt4(0) == 1);
    assertTrue(tuple.getText(1).equals("abc"));
    assertTrue(tuple.getFloat4(2) == 0.2f);
  }
}
