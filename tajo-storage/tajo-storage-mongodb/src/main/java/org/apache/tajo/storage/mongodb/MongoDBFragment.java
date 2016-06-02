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

/**
 * Created by janaka on 5/21/16.
 */
public class MongoDBFragment extends Fragment<Long> {

    protected MongoDBFragment(String kind, URI uri, String inputSourceId, Long startKey, Long endKey, long length, String[] hostNames) {
        super(kind, uri, inputSourceId, startKey, endKey, length, hostNames);
    }


    @Override
    public boolean isEmpty() {
        return false;
    }

}
