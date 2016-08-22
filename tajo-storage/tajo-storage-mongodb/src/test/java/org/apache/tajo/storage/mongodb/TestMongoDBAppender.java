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

import com.mongodb.client.MongoIterable;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SchemaBuilder;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.storage.*;
import org.bson.Document;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;

import static org.junit.Assert.assertEquals;

public class TestMongoDBAppender {


    static MongoDBTestServer server = MongoDBTestServer.getInstance();
    static URI uri = server.getURI();
    Appender appender;

    @Before
    public void setup() throws Exception {
        Tablespace space = TablespaceManager.getByName(server.SPACE_NAME);


        //Create Schema manually
        Schema schem = SchemaBuilder.builder().
                add(new Column("title", TajoDataTypes.Type.TEXT))
                .add(new Column("first_name", TajoDataTypes.Type.TEXT))
                .add(new Column("last_name", TajoDataTypes.Type.TEXT))
                .add(new Column("age", TajoDataTypes.Type.INT4))
                .add(new Column("height", TajoDataTypes.Type.FLOAT8))
                .add(new Column("single", TajoDataTypes.Type.BOOLEAN))
                .build();


        TableMeta meta = space.getMetadataProvider().getTableDesc(null, "got").getMeta();
        appender = space.getAppender(null, null, meta, schem, new Path(server.getURI() + "?" + MongoDBTableSpace.CONFIG_KEY_TABLE + "=got"));

        appender.init();

    }

    @AfterClass
    public static void tearDownClass() throws Exception {
       // server.stop();
    }

    @Test
    public void testAddTuple() throws IOException {

        //Create a tuple and add to  the table
        Tuple tuple = new VTuple(6);
        tuple.put(0, DatumFactory.createText("Kingslayer"));
        tuple.put(1, DatumFactory.createText("Jaime"));
        tuple.put(2, DatumFactory.createText("Lannister"));
        tuple.put(3, DatumFactory.createInt4(24));
        tuple.put(4, DatumFactory.createFloat8(165.98));
        tuple.put(5, DatumFactory.createBool(true));
        appender.addTuple(tuple);

        appender.flush();

        //Take data from server
        MongoIterable<Document> result = server.getMongoClient().getDatabase(server.DBNAME).getCollection("got").find(new Document("title", "Kingslayer"));
        Document doc = result.first();

        //Validate
        assertEquals(doc.get("title"), "Kingslayer");
        assertEquals(doc.get("first_name"), "Jaime");
        assertEquals(doc.get("last_name"), "Lannister");
        assertEquals(doc.get("age"), 24);
        assertEquals(doc.get("height"), 165.98);
        assertEquals(doc.get("single"), true);


        //Remove the inserted doc from database
        server.getMongoClient().getDatabase(server.DBNAME).getCollection("got").deleteOne(doc);
    }

}
