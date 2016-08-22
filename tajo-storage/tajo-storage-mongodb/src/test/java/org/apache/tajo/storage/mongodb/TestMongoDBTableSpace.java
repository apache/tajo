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

import com.google.common.collect.Sets;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoIterable;
import org.apache.tajo.catalog.SchemaBuilder;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.exception.TajoException;
import org.apache.tajo.exception.TajoRuntimeException;
import org.apache.tajo.schema.IdentifierUtil;
import org.apache.tajo.storage.Tablespace;
import org.apache.tajo.storage.TablespaceManager;
import org.junit.AfterClass;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.Assert.*;


public class TestMongoDBTableSpace {
  //mongodb://<dbuser>:<dbpassword>@ds017231.mlab.com:17231/tajo_test
  static MongoDBTestServer server = MongoDBTestServer.getInstance();
  static URI uri = server.getURI();


  @AfterClass
  public static void tearDownClass() throws Exception {
    //  server.stop();
  }

  @Test
  public void testTablespaceHandler() {
    assertTrue((TablespaceManager.getByName(server.SPACE_NAME)) instanceof MongoDBTableSpace);
    assertEquals(server.SPACE_NAME, (TablespaceManager.getByName(server.SPACE_NAME).getName()));

    assertTrue((TablespaceManager.get(uri.toASCIIString() + "&table=tb1")) instanceof MongoDBTableSpace);
    assertTrue((TablespaceManager.get(uri)) instanceof MongoDBTableSpace);


    //Test the URI same
    assertEquals(uri.toASCIIString(), TablespaceManager.get(uri).getUri().toASCIIString());
  }

  @Test(timeout = 1000, expected = TajoRuntimeException.class)
  public void testCreateTable() throws IOException, TajoException {
    Tablespace space = TablespaceManager.getByName(server.SPACE_NAME);
    space.createTable(null, false);
  }


  //Todo delete only metadat is provide
  @Test(timeout = 1000)
  public void testCreateTable_and_Purg() throws IOException, TajoException {
    Tablespace space = TablespaceManager.getByName(server.SPACE_NAME);

    TableDesc tableDesc = new TableDesc(
            IdentifierUtil.buildFQName(server.MAPPEDDBNAME, "Table1"),
            SchemaBuilder.builder()
                    .build(),
            null,
            server.getURI());

    //Test create and delete if meta data provided
    if (MongoDBTableSpace.STORAGE_PROPERTY.isMetadataProvided()) {
      space.createTable(tableDesc, false);

      //Check whether the created table is in the collection
      final Set<String> found = Sets.newHashSet(space.getMetadataProvider().getTables(null, null));
      assertTrue(found.contains("Table1"));

      //Check whether the created table is in the mongo database
      MongoClient mongoClient = server.getMongoClient();
      Boolean foundInMongoDB = false;
      MongoIterable<String> collectionNames = mongoClient.getDatabase(server.DBNAME).listCollectionNames();
      for (final String name : collectionNames) {
        if (name.equalsIgnoreCase("Table1")) {
          foundInMongoDB = true;
        }
      }
      assertTrue(foundInMongoDB);

      //Purg the table
      space.purgeTable(tableDesc);
      final Set<String> found_after = Sets.newHashSet(space.getMetadataProvider().getTables(null, null));
      assertFalse(found_after.contains("Table1"));

    }
  }

  @Test
  public void testTableVolume() throws IOException, TajoException {
    Tablespace space = TablespaceManager.getByName(server.SPACE_NAME);
    Map<String, Integer> tableSizes = new HashMap<String, Integer>();
    tableSizes.put("github", 4);
    tableSizes.put("got", 5);
    for (String tbl : server.collectionNames) {

      // assertEquals(1,space.getTableVolume(tableDesc, Optional.empty()));

      //  long a = 1;b
      TableDesc tbDesc = new TableDesc(
              IdentifierUtil.buildFQName(server.MAPPEDDBNAME, tbl),
              SchemaBuilder.builder()
                      .build(),
              null,
              space.getTableUri(null, null, tbl));

      assertEquals((int) tableSizes.get(tbl), space.getTableVolume(tbDesc, Optional.empty()));

    }
  }

}