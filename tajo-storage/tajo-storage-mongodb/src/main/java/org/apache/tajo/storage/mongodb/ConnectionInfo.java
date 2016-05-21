package org.apache.tajo.storage.mongodb;


public class ConnectionInfo {
    private String mongoDbUrl;
    public String getMongoDbUrl() {
        return mongoDbUrl;
    }

    public void setMongoDbUrl(String mongoDbUrl) {
        this.mongoDbUrl = mongoDbUrl;
    }
}
