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

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestConnectionInfo {

    @Test
    public final void testGetConnectionInfoType1() {

        ConnectionInfo connInfo = ConnectionInfo.fromURI("mongodb://localhost:1336/db1?table=tb1&user=testuser&password=testpass");

//        System.out.println(connInfo.scheme);
//        System.out.println(connInfo.dbName);
//        System.out.println(connInfo.host);
//        System.out.println(connInfo.port);
//        System.out.println(connInfo.tableName);
//        System.out.println(connInfo.password);
//        System.out.println(connInfo.user);
//        System.out.println(connInfo.mongoDbURI.getURI());

        assertEquals(connInfo.mongoDbURI.getURI(),"mongodb://testuser:testpass@localhost:1336/db1");
        assertEquals(connInfo.scheme, "mongodb");
        assertEquals(connInfo.host, "localhost");
        assertEquals(connInfo.port, 1336);
        assertEquals(connInfo.dbName, "db1");
        assertEquals(connInfo.user, "testuser");
        assertEquals(connInfo.password, "testpass");
        assertEquals(connInfo.tableName, "tb1");
    }

    @Test
    public final void testGetConnectionInfoType2() {
        //Create a connection info object
        ConnectionInfo connInfo = ConnectionInfo.fromURI("mongodb://localhost:1336/db1?table=tb1&user=testuser&password=testpass&TZ=GMT+9");
        assertEquals(connInfo.scheme, "mongodb");
        assertEquals(connInfo.host, "localhost");
        assertEquals(connInfo.port, 1336);
        assertEquals(connInfo.dbName, "db1");
        assertEquals(connInfo.user, "testuser");
        assertEquals(connInfo.password, "testpass");
        assertEquals(connInfo.tableName, "tb1");
        assertEquals(1, connInfo.params.size());
        assertEquals("GMT+9", connInfo.params.get("TZ"));
    }

    //Test code for multiple hosts
//    @Test
//    public final void testGetConnectionInfoType3() {
//
//        ConnectionInfo connInfo = ConnectionInfo.fromURI("mongodb://localhost:1336,googl.com:2727/db1?table=tb1&user=testuser&password=testpass&TZ=GMT+9");
//        assertEquals(connInfo.scheme, "mongodb");
//        assertEquals(connInfo.host, "localhost");
//        assertEquals(connInfo.port, 1336);
//        assertEquals(connInfo.dbName, "db1");
//        assertEquals(connInfo.user, "testuser");
//        assertEquals(connInfo.password, "testpass");
//        assertEquals(connInfo.tableName, "tb1");
//        assertEquals(1, connInfo.params.size());
//        assertEquals("GMT+9", connInfo.params.get("TZ"));
//    }

}
