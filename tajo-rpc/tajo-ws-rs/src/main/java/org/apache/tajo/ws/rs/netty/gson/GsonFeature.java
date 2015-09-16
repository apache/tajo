/**
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

package org.apache.tajo.ws.rs.netty.gson;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

import javax.ws.rs.core.Feature;
import javax.ws.rs.core.FeatureContext;
import javax.ws.rs.ext.MessageBodyReader;
import javax.ws.rs.ext.MessageBodyWriter;

import org.apache.tajo.json.GsonSerDerAdapter;

public class GsonFeature implements Feature {
  
  private Map<Type, GsonSerDerAdapter<?>> adapterMap;
  
  public GsonFeature() {
  }
  
  public GsonFeature(Map<Type, GsonSerDerAdapter<?>> adapterMap) {
    this.adapterMap = new HashMap<>(adapterMap.size());
    this.adapterMap.putAll(adapterMap);
  }

  @Override
  public boolean configure(FeatureContext featureContext) {
    if (adapterMap != null && !adapterMap.isEmpty()) {
      featureContext.register(new GsonReader().setAdapterMap(adapterMap), MessageBodyReader.class);
      featureContext.register(new GsonWriter().setAdapterMap(adapterMap), MessageBodyWriter.class);
    } else {
      featureContext.register(GsonReader.class, MessageBodyReader.class);
      featureContext.register(GsonWriter.class, MessageBodyWriter.class);
    }
    return true;
  }
}
