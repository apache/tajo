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

import com.google.common.base.Preconditions;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import io.netty.buffer.ByteBuf;
import net.minidev.json.JSONObject;
import net.minidev.json.parser.JSONParser;
import net.minidev.json.parser.ParseException;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.NestedPathUtil;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.plan.expr.EvalNode;
import org.apache.tajo.plan.logical.LogicalNode;
import org.apache.tajo.storage.Scanner;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;
import org.apache.tajo.storage.json.JsonLineDeserializer;
import org.apache.tajo.storage.text.TextLineParsingError;
import org.bson.Document;

import java.io.IOException;
import java.sql.DatabaseMetaData;
import java.util.Properties;

public class MongoDBScanner implements Scanner {

    private final DatabaseMetaData dbMetaData;
    private final Properties connProperties;
    private final String tableName;
    private final Schema schema;
    private final TableMeta tableMeta;
    private final MongoDBFragment fragment;
    private final TableStats stats;


    private ConnectionInfo connectionInfo;
    private JsonLineDeserializer deserializer;
    MongoDatabase db;
    Column[] targets;

    //this should be moved to another class
    private JSONParser parser;


    public MongoDBScanner(final DatabaseMetaData dbMetaData,
                          final Properties connProperties,
                          final Schema tableSchema,
                          final TableMeta tableMeta,
                          final MongoDBFragment fragment) {

        Preconditions.checkNotNull(dbMetaData);
        Preconditions.checkNotNull(connProperties);
        Preconditions.checkNotNull(tableSchema);
        Preconditions.checkNotNull(tableMeta);
        Preconditions.checkNotNull(fragment);

        this.dbMetaData = dbMetaData;
        this.connProperties = connProperties;
        this.tableName = ConnectionInfo.fromURI(fragment.getUri()).getTableName();
        this.schema = tableSchema;
        this.tableMeta = tableMeta;
        this.fragment = fragment;
        this.stats = new TableStats();
    }

    @Override
    public void init() throws IOException {
        connectionInfo = ConnectionInfo.fromURI(fragment.getUri());



        MongoClient mongoClient = new MongoClient(connectionInfo.getMongoDBURI());
        db = mongoClient.getDatabase(connectionInfo.getDbName());


        deserializer = new JsonLineDeserializer(schema, tableMeta, targets);
        deserializer.init();

    }

    int count =0;
    @Override
    public Tuple next() throws IOException {
        MongoIterable<Document> iter= db.getCollection(tableName).find();


        if(count>=2) return null;

        //This should be moved to a seperate class
        Tuple tp = new VTuple(2);
        tp.put(1, DatumFactory.createText("Lol"));
        count++;
        return tp;
    }

    @Override
    public void reset() throws IOException {
        count=0;
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public void pushOperators(LogicalNode planPart) {

    }

    @Override
    public boolean isProjectable() {
        return false;
    }

    @Override
    public void setTarget(Column[] targets) {
        this.targets= targets;
    }

    @Override
    public boolean isSelectable() {
        return false;
    }

    @Override
    public void setFilter(EvalNode filter) {

    }

    @Override
    public void setLimit(long num) {

    }

    @Override
    public boolean isSplittable() {
        return false;
    }

    @Override
    public float getProgress() {
        return 0;
    }

    @Override
    public TableStats getInputStats() {
        return null;
    }

    @Override
    public Schema getSchema() {
        return null;
    }
}
