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

import org.apache.tajo.QueryTestCaseBase;
import org.apache.tajo.exception.TajoException;
import org.junit.*;

import java.net.URI;

public class TestMongoDBQueryTest  extends QueryTestCaseBase{

    static MongoDBTestServer server = MongoDBTestServer.getInstance();
    static URI uri = server.getURI();

    public TestMongoDBQueryTest() {
        super(server.MAPPEDDBNAME);
    }

    @BeforeClass
    public static void setup() throws  Exception
    {
        QueryTestCaseBase.testingCluster.getMaster().refresh();
      //  TablespaceManager.addTableSpaceForTest(new ExampleHttpFileTablespace("http_example", uri, configElements));

    }

    @AfterClass
    public static void tearDownClass() throws Exception  {
        server.stop();
    }

    @Before
    public void prepareTables() throws TajoException {
        if(!MongoDBTableSpace.STORAGE_PROPERTY.isMetadataProvided()) {
            executeString("create table got (title,first_name,last_name) tablespace test_spacename using mongodb");
            executeString("create table github (*) tablespace test_spacename using mongodb");
            //   executeString("create table github (*) tablespace test_spacename using ex_http_json with ('path'='github.json')");
        }
    }

    @After
    public void dropTables() throws TajoException {
        if(!MongoDBTableSpace.STORAGE_PROPERTY.isMetadataProvided()) {
            executeString("drop table got");
            executeString("drop table github");
        }
    }


    @SimpleTest
    @Test
    public void testSelect() throws Exception {
        runSimpleTests();
    }


    @SimpleTest
    @Test
    public void testSort() throws Exception {
        runSimpleTests();
    }

    @SimpleTest
    @Test
    public void testGroupby() throws Exception {
        runSimpleTests();
    }

    @SimpleTest
    @Test
    public void testJoin() throws Exception {
        runSimpleTests();
    }


    @SimpleTest
    @Test
    public void testInsert() throws Exception {
//        runSimpleTests();
    }
}
