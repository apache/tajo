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
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.tajo.storage.json.JsonLineDeserializer;
import org.bson.Document;

import java.io.IOException;

public class MongoDBCollectionReader {
    private MongoCollection<Document> collection;
    private ConnectionInfo connectionInfo;
    private JsonLineDeserializer deserializer;

    public MongoDBCollectionReader(ConnectionInfo connectionInfo,JsonLineDeserializer deserializer)
    {
        this.connectionInfo = connectionInfo;
        this.deserializer = deserializer;
    }

    public void init() throws IOException
    {
        MongoClient mongoClient = new MongoClient(connectionInfo.getMongoDBURI());
        MongoDatabase db = mongoClient.getDatabase(connectionInfo.getDbName());
        collection = db.getCollection(connectionInfo.getTableName());
    }

}
