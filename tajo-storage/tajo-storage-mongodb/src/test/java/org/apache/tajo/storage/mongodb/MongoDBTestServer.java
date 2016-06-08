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

/**
 * Created by janaka on 6/7/16.
 */
import com.mongodb.*;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.util.JSON;
import com.sun.javadoc.Doc;
import de.flapdoodle.embed.mongo.*;
import de.flapdoodle.embed.mongo.config.*;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.process.runtime.Network;
import net.minidev.json.JSONObject;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.htrace.fasterxml.jackson.core.JsonFactory;
import org.apache.htrace.fasterxml.jackson.core.JsonParser;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.storage.TablespaceManager;
import org.bson.Document;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class MongoDBTestServer  {

    private static final Log LOG = LogFactory.getLog(MongoDBTableSpace.class);


    public static int port = 12345;
    public static String host = "localhost";
    public static String dbName = "test_dbname";
    public static String mappedDbName = "test_mapped_dbname";
    private static MongoDBTestServer instance;

    public static final String spaceName = "test_spacename";


    //New
    private static final MongodStarter starter = MongodStarter.getDefaultInstance();
    private MongodExecutable _mongodExe;
    private MongodProcess _mongod;
    private MongoClient _mongo;

    private String[] filenames = {"file1.json","file2.json"};
    public String[] collectionNames = {"col1","col2"};


    public static MongoDBTestServer getInstance()
    {
        if(instance==null)
        {
            try {
                instance = new MongoDBTestServer();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        return instance;
    }

    private MongoDBTestServer () throws IOException, URISyntaxException {
        _mongodExe = starter.prepare(new MongodConfigBuilder()
                .version(Version.Main.PRODUCTION)
                .net(new Net(port, Network.localhostIsIPv6()))

                .cmdOptions( new MongoCmdOptionsBuilder().
                        useStorageEngine("mmapv1").
                        build())
                .build());
        _mongod = _mongodExe.start();
        _mongo = new MongoClient(host, port);
        registerTablespace();

        loadData();
    }

    public void stop()
    {
        _mongod.stop();
        _mongodExe.stop();
    }

    public URI getURI()
    {
        try {
            return new URI("mongodb://" + host + ":" + port + "/"+dbName);
        }
        catch (Exception e)
        {
            return null;
        }
    }


    //From GitHub
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

    private void registerTablespace() throws IOException {
        JSONObject configElements = new JSONObject();
        configElements.put(MongoDBTableSpace.CONFIG_KEY_MAPPED_DATABASE, mappedDbName);

        MongoDBTableSpace tablespace = new MongoDBTableSpace(spaceName,getURI(),configElements);
        tablespace.init(new TajoConf());


        TablespaceManager.addTableSpaceForTest(tablespace);
    }

    private void loadData() throws IOException, URISyntaxException {
        MongoDatabase db =  _mongo.getDatabase(dbName);
        for(int i=0; i<filenames.length;i++) {

            db.createCollection(collectionNames[i]);
            MongoCollection coll = db.getCollection(collectionNames[i]);

            String fileContent = new Scanner( getRequestedFile(filenames[i])).useDelimiter("\\Z").next();

            Document fileDoc = Document.parse(fileContent);
//            System.out.println( fileDoc.get("dataList"));
//            coll.insertMany((List<Document>) fileDoc.get("dataList"));
           coll.insertOne(fileDoc);

             FindIterable<Document> docs =  coll.find();

                docs.forEach(new Block<Document>() {
                    @Override
                    public void apply(final Document document) {
                        LOG.info(document.toJson());
                    }
                });
        }
    }

    private static File getRequestedFile(String path) throws FileNotFoundException, URISyntaxException {

        URL url = ClassLoader.getSystemResource("datasets/" + path);

        if (url == null) {
            throw new FileNotFoundException(path);
        }
        return new File(url.toURI());
    }
}
