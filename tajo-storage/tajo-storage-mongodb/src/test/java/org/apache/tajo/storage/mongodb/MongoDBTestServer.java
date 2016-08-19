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


import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import de.flapdoodle.embed.mongo.*;
import de.flapdoodle.embed.mongo.config.*;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.process.runtime.Network;
import net.minidev.json.JSONObject;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.storage.TablespaceManager;
import org.bson.Document;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class MongoDBTestServer {

    //mongo server details
    public static final int PORT = 12345;
    public static final String HOST = "localhost";
    public static final String DBNAME = "test_dbname";
    public static final String MAPPEDDBNAME = "test_mapped_dbname";
    //tajo tableSpace name
    public static final String SPACE_NAME = "test_spacename";
    private static final Log LOG = LogFactory.getLog(MongoDBTableSpace.class);
    //Mongo Server componenets
    private static final MongodStarter starter = MongodStarter.getDefaultInstance();
    // instance for singleton server
    private static MongoDBTestServer instance;
    //Collection names (table names) to be created inside mongodb
    public String[] collectionNames = {"github", "got"};
    private MongodExecutable _mongodExe;
    private MongodProcess _mongod;
    private MongoClient _mongo;
    //File names to load data
    private String[] filenames = {"file1.json", "file2.json"};


    // private constructor
    private MongoDBTestServer() throws IOException, URISyntaxException {
        _mongodExe = starter.prepare(new MongodConfigBuilder()
                .version(Version.Main.PRODUCTION)
                .net(new Net(PORT, Network.localhostIsIPv6()))

                .cmdOptions(new MongoCmdOptionsBuilder().
                        useStorageEngine("mmapv1").
                        build())
                .build());
        _mongod = _mongodExe.start();
        _mongo = new MongoClient(HOST, PORT);
        registerTablespace();

        loadData();
    }

    //server object can be created using this method
    public static MongoDBTestServer getInstance() {
        if (instance == null) {
            try {
                instance = new MongoDBTestServer();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        return instance;
    }

    //To load files
    private static File getRequestedFile(String path) throws FileNotFoundException, URISyntaxException {

        URL url = ClassLoader.getSystemResource("dataset/" + path);

        if (url == null) {
            throw new FileNotFoundException(path);
        }
        return new File(url.toURI());
    }

    //To stop the server
    public void stop() {
        _mongod.stop();
        _mongodExe.stop();
        instance = null;
    }

    //Returns the url which can be used to connet to the server instance
    public URI getURI() {
        try {
            return new URI("mongodb://" + HOST + ":" + PORT + "/" + DBNAME);
        } catch (Exception e) {
            return null;
        }
    }

    //Start mongo process
    private MongosProcess startMongos(int port, int defaultConfigPort, String defaultHost) throws UnknownHostException,
            IOException {
        IMongosConfig mongosConfig = new MongosConfigBuilder()
                .version(Version.Main.PRODUCTION)
                .net(new Net(port, Network.localhostIsIPv6()))
                .configDB(defaultHost + ":" + defaultConfigPort)
                .build();

        MongosExecutable mongosExecutable = MongosStarter.getDefaultInstance().prepare(mongosConfig);
        MongosProcess mongos = mongosExecutable.start();
        return mongos;
    }

    private MongodProcess startMongod(int defaultConfigPort) throws UnknownHostException, IOException {
        IMongodConfig mongoConfigConfig = new MongodConfigBuilder()
                .version(Version.Main.PRODUCTION)
                .net(new Net(defaultConfigPort, Network.localhostIsIPv6()))
                .configServer(true)
                .build();

        MongodExecutable mongodExecutable = MongodStarter.getDefaultInstance().prepare(mongoConfigConfig);
        MongodProcess mongod = mongodExecutable.start();
        return mongod;
    }

    //Register the new table space for testing
    private void registerTablespace() throws IOException {
        JSONObject configElements = new JSONObject();
        configElements.put(MongoDBTableSpace.CONFIG_KEY_MAPPED_DATABASE, MAPPEDDBNAME);

        MongoDBTableSpace tablespace = new MongoDBTableSpace(SPACE_NAME, getURI(), configElements);
        tablespace.init(new TajoConf());


        TablespaceManager.addTableSpaceForTest(tablespace);
    }

    //Create tables and Load data into the mongo server instance
    private void loadData() throws IOException, URISyntaxException {
        MongoDatabase db = _mongo.getDatabase(DBNAME);
        for (int i = 0; i < filenames.length; i++) {

            db.createCollection(collectionNames[i]);
            MongoCollection coll = db.getCollection(collectionNames[i]);

            String fileContent = new Scanner(getRequestedFile(filenames[i])).useDelimiter("\\Z").next();

            //Document list
            List<Document> documentList = new ArrayList<Document>();
            try {
                JSONArray jsonarray = new JSONArray(fileContent);
                for (int j = 0; j < jsonarray.length(); j++) {
                    String jsonStr = jsonarray.getJSONObject(j).toString();
                    documentList.add(Document.parse(jsonStr));
                }

            } catch (JSONException e) {
                e.printStackTrace();
            }

            coll.insertMany(documentList);

            FindIterable<Document> docs = coll.find();

            docs.forEach(new Block<Document>() {
                @Override
                public void apply(final Document document) {
                    LOG.info(document.toJson());
                }
            });
        }
    }

    //Return a mongo client which directly connect to the mongo database.
    public MongoClient getMongoClient() {
        return _mongo;
    }
}
