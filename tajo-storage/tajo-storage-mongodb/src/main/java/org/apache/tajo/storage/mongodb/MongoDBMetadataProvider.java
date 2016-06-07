package org.apache.tajo.storage.mongodb;

import org.apache.tajo.catalog.MetadataProvider;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.exception.UndefinedTablespaceException;

import javax.annotation.Nullable;
import java.net.URI;
import java.util.Collection;
import java.util.Collections;

/**
 * Created by janaka on 6/7/16.
 */
public class MongoDBMetadataProvider implements MetadataProvider {

    private MongoDBTableSpace tableSpace;
    private String dbName;

    public MongoDBMetadataProvider(MongoDBTableSpace tableSpace, String dbName)
    {
        this.tableSpace = tableSpace;
        this.dbName = dbName;
    }

    @Override
    public String getTablespaceName() {
        return tableSpace.getName();
    }

    @Override
    public URI getTablespaceUri() {
        return tableSpace.getUri();
    }

    @Override
    public String getDatabaseName() {
        return dbName;
    }

    @Override
    public Collection<String> getSchemas() {
        return Collections.EMPTY_SET;
    }

    @Override
    public Collection<String> getTables(@Nullable String schemaPattern, @Nullable String tablePattern) {
        return Collections.EMPTY_SET;
    }

    @Override
    public TableDesc getTableDesc(String schemaName, String tableName) throws UndefinedTablespaceException {
        return null;
    }
}
