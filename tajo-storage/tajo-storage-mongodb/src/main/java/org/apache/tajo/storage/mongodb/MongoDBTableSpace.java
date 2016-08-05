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

import com.google.common.collect.Lists;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import net.minidev.json.JSONObject;
import org.apache.avro.generic.GenericData;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.OverridableConf;
import org.apache.tajo.catalog.*;
import org.apache.tajo.exception.*;
import org.apache.tajo.plan.LogicalPlan;
import org.apache.tajo.plan.expr.EvalNode;
import org.apache.tajo.plan.logical.LogicalNode;
import org.apache.tajo.rpc.protocolrecords.PrimitiveProtos;
import org.apache.tajo.schema.IdentifierUtil;
import org.apache.tajo.storage.FormatProperty;
import org.apache.tajo.storage.StorageProperty;
import org.apache.tajo.storage.Tablespace;
import org.apache.tajo.storage.TupleRange;
import org.apache.tajo.storage.fragment.Fragment;
import org.bson.Document;

import javax.annotation.Nullable;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/*
* TableSpace for MongoDB
* */

public class MongoDBTableSpace extends Tablespace {

    private static final Log LOG = LogFactory.getLog(MongoDBTableSpace.class);


    //Table Space Properties
    static final StorageProperty STORAGE_PROPERTY = new StorageProperty("rowstore", // type is to be defined
            false,  //not movable
            true,   //writable at the moment
            true,   // Absolute path
            false); // Meta data will  be provided
    static final FormatProperty FORMAT_PROPERTY = new FormatProperty(
            false, // Insert
            false, //direct insert
            false);// result staging

    //Mongo Client object
    private ConnectionInfo connectionInfo;
    protected MongoClient mongoClient;
    protected MongoDatabase db;
    protected String mappedDBName;


    //Config Keys
    public static final String CONFIG_KEY_MAPPED_DATABASE = "mapped_database";
    public static final String CONFIG_KEY_CONN_PROPERTIES = "connection_properties";
    public static final String CONFIG_KEY_USERNAME = "user";
    public static final String CONFIG_KEY_PASSWORD = "password";

    public MongoDBTableSpace(String name, URI uri, JSONObject config) {

        super(name, uri, config);
        connectionInfo = ConnectionInfo.fromURI(uri);

        //set Connection Properties
        if (config.containsKey(CONFIG_KEY_MAPPED_DATABASE)) {
            mappedDBName = this.config.getAsString(CONFIG_KEY_MAPPED_DATABASE);
        } else {
            mappedDBName = getConnectionInfo().getDbName();
        }



    }

    @Override
    protected void storageInit() throws IOException {
        //Todo Extract User Details from Configuration
        try {
            connectionInfo = ConnectionInfo.fromURI(uri);
            mongoClient = new MongoClient(getConnectionInfo().getMongoDBURI());
            db = mongoClient.getDatabase(getConnectionInfo().getDbName());
        } catch (Exception e) {
            throw new TajoInternalError(e);
        }
    }

    @Override
    public long getTableVolume(TableDesc table, Optional<EvalNode> filter) {

        long count = 0;
        try {
            String[] nameSplited =  IdentifierUtil.splitFQTableName(table.getName());
            count = db.getCollection(nameSplited[1]).count();
        } catch (Exception e) {
            throw new TajoInternalError(e);
        }
        return count;
    }



    @Override
    public List<Fragment> getSplits(String inputSourceId, TableDesc tableDesc, boolean requireSort, @Nullable EvalNode filterCondition) throws IOException, TajoException {
        long tableVolume = getTableVolume(tableDesc, Optional.empty());
        MongoDBFragment mongoDBFragment = new MongoDBFragment(tableDesc.getUri(), inputSourceId, 0, tableVolume );
        return Lists.newArrayList(mongoDBFragment);
    }


    @Override
    public StorageProperty getProperty() {
        return STORAGE_PROPERTY;
    }

    @Override
    public FormatProperty getFormatProperty(TableMeta meta) {
        return FORMAT_PROPERTY;
    }

    @Override
    public void close() {

    }

    @Override
    public TupleRange[] getInsertSortRanges(OverridableConf queryContext, TableDesc tableDesc, Schema inputSchema, SortSpec[] sortSpecs, TupleRange dataRange) throws IOException {
        return new TupleRange[0];
    }

    @Override
    public void verifySchemaToWrite(TableDesc tableDesc, Schema outSchema) throws TajoException {

    }

    @Override
    public void createTable(TableDesc tableDesc, boolean ifNotExists) throws TajoException, IOException {
        if(tableDesc==null)
            throw new TajoRuntimeException(new NotImplementedException());
        MongoCollection<Document> table = db.getCollection(tableDesc.getName());

        //TODO Handle this here. If empty throw exception or what?
        boolean ifExist = (table.count()>0)?true:false;

        //If meta  data provides. Create a table
        if(STORAGE_PROPERTY.isMetadataProvided())
            db.createCollection(IdentifierUtil.extractSimpleName(tableDesc.getName()));
    }

    @Override
    public void purgeTable(TableDesc tableDesc) throws IOException, TajoException {
        if(STORAGE_PROPERTY.isMetadataProvided())
            db.getCollection(tableDesc.getName()).drop();
    }

    @Override
    public void prepareTable(LogicalNode node) throws IOException, TajoException {
        return;
    }

    @Override
    public Path commitTable(OverridableConf queryContext, ExecutionBlockId finalEbId, LogicalPlan plan, Schema schema, TableDesc tableDesc) throws IOException {
        return null;
    }

    @Override
    public void rollbackTable(LogicalNode node) throws IOException, TajoException {

    }

    @Override
    public URI getStagingUri(OverridableConf context, String queryId, TableMeta meta) throws IOException {
        return null;
    }

    @Override
    public URI getRootUri() {
        return uri;
    }

    @Override
    public URI getTableUri(TableMeta meta, String databaseName, String tableName) {
        //ToDo Find a better way this
        String tableURI = "";
        if(this.getUri().toASCIIString().contains("?"))
            tableURI = this.getUri().toASCIIString()+"&table="+tableName;
        else
            tableURI = this.getUri().toASCIIString()+"?table="+tableName;

        return URI.create(tableURI);
    }

    //@Override
    public URI getTableUri( String databaseName, String tableName) {
        //ToDo set the TableURI properly
        return URI.create(this.getUri()+"&table="+tableName);
    }

//    @Override
//    public URI getTableUri(String databaseName, String tableName) {
//        //ToDo set the TableURI properly
//        return URI.create(this.getUri()+"&table="+tableName);
//    }


   // Metadata
    public MetadataProvider getMetadataProvider() {
        return new MongoDBMetadataProvider(this, mappedDBName);
    }

    public ConnectionInfo getConnectionInfo() {
        return connectionInfo;
    }
}
