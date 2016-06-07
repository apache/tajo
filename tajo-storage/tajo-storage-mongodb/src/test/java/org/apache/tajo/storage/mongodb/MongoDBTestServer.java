package org.apache.tajo.storage.mongodb;

/**
 * Created by janaka on 6/7/16.
 */
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodProcess;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.ArtifactStoreBuilder;
import de.flapdoodle.embed.mongo.config.IMongodConfig;
import de.flapdoodle.embed.mongo.config.MongodConfigBuilder;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.process.runtime.Network;

import java.io.IOException;
import java.net.URI;
import java.util.Date;

public class MongoDBTestServer {
    private static int port;
    private static String host = "localhost";
    private static String dbName;
    private static MongoDBTestServer instance;

    private MongodExecutable mongodExecutable;

    static {
        try {
            instance = new MongoDBTestServer();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static MongoDBTestServer getInstance()
    {
        return instance;
    }

    private MongoDBTestServer () throws IOException {
        MongodStarter starter = MongodStarter.getDefaultInstance();


        IMongodConfig mongodConfig = new MongodConfigBuilder()
                .version(Version.Main.PRODUCTION)
                .net(new Net(port, Network.localhostIsIPv6()))
                .build();

            mongodExecutable = starter.prepare(mongodConfig);
            MongodProcess mongod = mongodExecutable.start();

            MongoClient mongo = new MongoClient(host, port);
            DB db = mongo.getDB(dbName);
            DBCollection col = db.createCollection("testCol", new BasicDBObject());
            col.save(new BasicDBObject("testDoc", new Date()));


    }

    public void stop()
    {
        mongodExecutable.stop();
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
}
