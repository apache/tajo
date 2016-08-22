/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.storage.mongodb;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import org.apache.tajo.catalog.MetadataProvider;
import org.apache.tajo.catalog.SchemaBuilder;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.exception.UndefinedTablespaceException;
import org.apache.tajo.schema.IdentifierUtil;
import org.apache.tajo.util.KeyValueSet;

import javax.annotation.Nullable;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

/*
* Used to provide MetaData about a particular mongodb tablespace.
* Provides table details, list of tables and statistics of tables
* * */
public class MongoDBMetadataProvider implements MetadataProvider {

  MongoDatabase db;
  private MongoDBTableSpace tableSpace;
  private String mappedDbName;
  private ConnectionInfo connectionInfo;

  public MongoDBMetadataProvider(MongoDBTableSpace tableSpace, String dbName) {
    this.tableSpace = tableSpace;
    this.mappedDbName = dbName;

    connectionInfo = tableSpace.getConnectionInfo();
    MongoClient mongoClient = new MongoClient(connectionInfo.getMongoDBURI());
    db = mongoClient.getDatabase(connectionInfo.getDbName());
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
    return mappedDbName;
  }

  @Override
  public Collection<String> getSchemas() {
    return Collections.EMPTY_SET;
  }

  @Override
  public Collection<String> getTables(@Nullable String schemaPattern, @Nullable String tablePattern) {

    //Get a list of table(=collection) names
    MongoIterable<String> collectionList = db.listCollectionNames();

    //Map to a string list and return
    Collection<String> list = new ArrayList<String>();
    for (String item : collectionList) {
      list.add(item);
    }
    return list;
  }

  @Override
  public TableDesc getTableDesc(String schemaName, String tableName) throws UndefinedTablespaceException {

    //Create table description and meta fro a specific table
    TableMeta tbMeta = new TableMeta("mongodb", new KeyValueSet());
    TableDesc tbDesc = new TableDesc(
            IdentifierUtil.buildFQName(mappedDbName, tableName),
            SchemaBuilder.builder()
//                        .add(new Column("title", TajoDataTypes.Type.TEXT))
//                        .add(new Column("first_name", TajoDataTypes.Type.TEXT))
//                        .add(new Column("last_name", TajoDataTypes.Type.TEXT))
                    .build(),
            tbMeta,
            tableSpace.getTableUri(null, null, tableName));

    final TableStats stats = new TableStats();
    stats.setNumRows(-1); // unknown

    //Set the raw count
    stats.setNumRows(db.getCollection(tableName).count());


    tbDesc.setStats(stats);
    return tbDesc;
  }
}
