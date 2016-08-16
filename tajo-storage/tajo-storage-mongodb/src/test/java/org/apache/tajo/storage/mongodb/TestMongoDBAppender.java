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
import org.apache.tajo.QueryTestCaseBase;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SchemaBuilder;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.storage.*;
import org.bson.Document;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;

import static org.junit.Assert.assertEquals;

public class TestMongoDBAppender {


    static MongoDBTestServer server = MongoDBTestServer.getInstance();
    static URI uri = server.getURI();
    Appender appender;

    @Before
    public void setup() throws  Exception
    {
        Tablespace space = TablespaceManager.getByName(server.SPACE_NAME);


        //Create Schema manually
        Schema schem = SchemaBuilder.builder().
                add(new Column("title", TajoDataTypes.Type.TEXT))
                .add(new Column("first_name", TajoDataTypes.Type.TEXT))
                .add(new Column("last_name", TajoDataTypes.Type.TEXT))
                .build();


        TableMeta meta = space.getMetadataProvider().getTableDesc(null,"got").getMeta();
        appender = space.getAppender(null,null,meta,schem,new Path(server.getURI()+"?"+MongoDBTableSpace.CONFIG_KEY_TABLE+"=got") );

        appender.init();

    }

    @Test
    public void testAddTupleText() throws IOException {

        //Create a tuple and add to  the table
        Tuple tuple = new VTuple(3);
        tuple.put(0, DatumFactory.createText("Good_Man"));
        tuple.put(1,DatumFactory.createText("Janaka"));
        tuple.put(2,DatumFactory.createText("Chathuranga"));
        appender.addTuple(tuple);

        //Take data from server
        MongoIterable<Document> result = server.getMongoClient().getDatabase(server.DBNAME).getCollection("got").find(new Document("title","Good_Man"));
        Document doc = result.first();

        //Validate
        assertEquals(doc.get("title"),"Good_Man");
        assertEquals(doc.get("first_name"),"Janaka");
        assertEquals(doc.get("last_name"),"Chathuranga");


        //Remove the inserted doc from database
        server.getMongoClient().getDatabase(server.DBNAME).getCollection("got").deleteOne(doc);
    }



}
