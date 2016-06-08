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

import org.apache.tajo.storage.TablespaceManager;
import org.junit.After;
import org.junit.Test;

import java.net.URI;
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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by janaka on 6/2/16.
 */
public class TestTableSpace {
    //mongodb://<dbuser>:<dbpassword>@ds017231.mlab.com:17231/tajo_test
    static MongoDBTestServer server = MongoDBTestServer.getInstance();
    static URI uri = server.getURI();

    @Test
    public void testTablespaceHandler()
    {
        assertTrue((TablespaceManager.get(uri)) instanceof MongoDBTableSpace);

        assertTrue((TablespaceManager.getByName(server.spaceName)) instanceof MongoDBTableSpace);
        assertEquals(server.spaceName, (TablespaceManager.getByName(server.spaceName).getName()));
        assertTrue((TablespaceManager.get(uri.toASCIIString() + "&table=tb1")) instanceof MongoDBTableSpace);


        //Test the URI same
        assertEquals(uri.toASCIIString(), TablespaceManager.get(uri).getUri().toASCIIString());

        //Check if returns the  MetaDataProvider
        assertTrue(TablespaceManager.get(uri).getMetadataProvider() instanceof MongoDBMetadataProvider);
    }
}