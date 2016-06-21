/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.tajo.storage.mongodb;

import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.storage.fragment.Fragment;

import java.net.URI;
import java.util.List;


public class MongoDBFragment extends Fragment<Long> {

    protected MongoDBFragment(String kind, URI uri, String inputSourceId, Long startKey, Long endKey, long length, String[] hostNames) {
        super(kind, uri, inputSourceId, startKey, endKey, length, hostNames);
    }


    public MongoDBFragment(String inputSourceId, URI uri, List<String> hostNames) {


        super("MONGODB", uri, inputSourceId, null, null, 0, toArray(hostNames));
    }

    public static String[] toArray(List<String> list)
    {
        String[] hosts = new String[1];
        hosts[0] = list.get(0);
        return hosts;
    }

    public MongoDBFragment(String inputSourceId, URI uri, String[] hostNames) {
        super("MONGODB", uri, inputSourceId, null, null, 0, hostNames);
    }

    @Override
    public boolean isEmpty() {
        return true;
    }

}
