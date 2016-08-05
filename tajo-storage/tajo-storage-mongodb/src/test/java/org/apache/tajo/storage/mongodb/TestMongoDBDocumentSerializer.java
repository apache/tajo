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

import com.sun.org.apache.xml.internal.security.encryption.DocumentSerializer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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

public class TestMongoDBDocumentSerializer {


    private static final Log LOG = LogFactory.getLog(MongoDBTableSpace.class);

    @Test
    public void testSeriaalizeTextType() throws IOException {
        Schema schem = SchemaBuilder.builder().
                add(new Column("title", TajoDataTypes.Type.TEXT))
                .add(new Column("first_name", TajoDataTypes.Type.TEXT))
                .add(new Column("last_name", TajoDataTypes.Type.TEXT))
                .build();

        MongoDBDocumentSerializer dc = new MongoDBDocumentSerializer(schem,null);

        Tuple tuple = new VTuple(3);
        tuple.put(0,DatumFactory.createText("Good_Man"));
        tuple.put(1,DatumFactory.createText("Janaka"));
        tuple.put(2,DatumFactory.createText("Chathuranga"));
        Document md = new Document();

        dc.deserialize(tuple,md);

        LOG.info(md.toJson());

    }

}
