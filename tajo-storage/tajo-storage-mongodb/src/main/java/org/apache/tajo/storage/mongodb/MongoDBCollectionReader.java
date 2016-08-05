/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.tajo.storage.mongodb;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import io.netty.util.CharsetUtil;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;
import org.apache.tajo.storage.text.TextLineParsingError;
import org.bson.Document;

import java.io.IOException;

import java.nio.charset.CharsetEncoder;
import java.util.ArrayList;
import java.util.List;

/*
    Reads data from a mongodb collection, line by line.
    Used within the MongoScanner to read tuples.
 */
public class MongoDBCollectionReader {
    private ConnectionInfo connectionInfo;
    private MongoDBDocumentDeserializer deserializer;
    private int targetLength;
    List<Document> documentList;
    private int currentIndex;
    private final CharsetEncoder encoder = CharsetUtil.getEncoder(CharsetUtil.UTF_8);


    public MongoDBCollectionReader(ConnectionInfo connectionInfo, MongoDBDocumentDeserializer deserializer , int targetLength)
    {
        this.connectionInfo = connectionInfo;
        this.deserializer = deserializer;
        this.targetLength = targetLength;
    }

    public void init() throws IOException
    {
        currentIndex = 0;
        MongoClient mongoClient = new MongoClient(connectionInfo.getMongoDBURI());
        MongoDatabase db = mongoClient.getDatabase(connectionInfo.getDbName());

        MongoCollection<Document> collection = db.getCollection(connectionInfo.getTableName());
        documentList =(List<Document>) collection.find().into(
                new ArrayList<Document>());;

        deserializer.init();
    }

    public Tuple readTuple() throws IOException, TextLineParsingError {
        if(currentIndex>=documentList.size()) return null;

        Tuple outTuple = new VTuple(targetLength);

        deserializer.deserialize(documentList.get(currentIndex), outTuple);
        currentIndex++;
        return outTuple;

    }

    public float getProgress()
    {
        return ((float)currentIndex) / documentList.size();
    }

}
