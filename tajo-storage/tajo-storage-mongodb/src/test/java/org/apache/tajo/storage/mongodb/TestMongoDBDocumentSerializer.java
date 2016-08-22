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
package org.apache.tajo.storage.mongodb;

import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SchemaBuilder;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;
import org.bson.Document;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class TestMongoDBDocumentSerializer {

  @Test
  public void testSeriaalizeTextType() throws IOException {
    Schema schem = SchemaBuilder.builder().
            add(new Column("title", TajoDataTypes.Type.TEXT))
            .add(new Column("first_name", TajoDataTypes.Type.TEXT))
            .add(new Column("last_name", TajoDataTypes.Type.TEXT))
            .build();

    MongoDBDocumentSerializer documentSerializer = new MongoDBDocumentSerializer(schem, null);

    Tuple tuple = new VTuple(3);
    tuple.put(0, DatumFactory.createText("Kingslayer"));
    tuple.put(1, DatumFactory.createText("Jaime"));
    tuple.put(2, DatumFactory.createText("Lannister"));
    Document md = new Document();

    documentSerializer.serialize(tuple, md);

    assertEquals("Kingslayer", md.getString("title"));
    assertEquals("Jaime", md.getString("first_name"));
    assertEquals("Lannister", md.getString("last_name"));

  }

  @Test
  public void testSerializeIntFloatBoolean() throws IOException {
    Schema schem = SchemaBuilder.builder().
            add(new Column("title", TajoDataTypes.Type.TEXT))
            .add(new Column("age", TajoDataTypes.Type.INT4))
            .add(new Column("height", TajoDataTypes.Type.FLOAT8))
            .add(new Column("single", TajoDataTypes.Type.BOOLEAN))
            .build();

    MongoDBDocumentSerializer documentSerializer = new MongoDBDocumentSerializer(schem, null);

    Tuple tuple = new VTuple(4);
    tuple.put(0, DatumFactory.createText("Mr"));
    tuple.put(1, DatumFactory.createInt4(24));
    tuple.put(2, DatumFactory.createFloat8(165.98));
    tuple.put(3, DatumFactory.createBool(true));
    Document md = new Document();

    documentSerializer.serialize(tuple, md);

    assertEquals("Mr", md.get("title"));
    assertEquals(24, md.get("age"));
    assertEquals(165.98, md.get("height"));
    assertEquals(true, md.get("single"));

  }

}
