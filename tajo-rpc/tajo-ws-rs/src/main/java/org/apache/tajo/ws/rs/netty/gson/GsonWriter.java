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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyWriter;

import org.apache.tajo.json.GsonSerDerAdapter;

import java.io.*;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.util.Map;
import java.util.Map.Entry;

/**
 * custom message body writer with Gson feature.
 */
@Produces(MediaType.APPLICATION_JSON)
public class GsonWriter<T> implements MessageBodyWriter<T> {
  
  private Map<Type, GsonSerDerAdapter<?>> adapterMap;
  
  public GsonWriter<T> setAdapterMap(Map<Type, GsonSerDerAdapter<?>> adapterMap) {
    this.adapterMap = adapterMap;
    return this;
  }

  @Override
  public boolean isWriteable(Class<?> aClass, Type type, Annotation[] annotations, MediaType mediaType) {
    return GsonUtil.isJsonType(mediaType);
  }

  @Override
  public long getSize(T t, Class<?> aClass, Type type, Annotation[] annotations, MediaType mediaType) {
    return 0;
  }

  @Override
  public void writeTo(T t, Class<?> aClass, Type type, Annotation[] annotations, MediaType mediaType,
                      MultivaluedMap<String, Object> multivaluedMap, OutputStream outputStream)
      throws IOException, WebApplicationException {
    Gson gson;
    if (adapterMap != null && !adapterMap.isEmpty()) {
      GsonBuilder gsonBuilder = new GsonBuilder().excludeFieldsWithoutExposeAnnotation();
      for (Entry<Type, GsonSerDerAdapter<?>> adapter: adapterMap.entrySet()) {
        gsonBuilder.registerTypeAdapter(adapter.getKey(), adapter.getValue());
      }
      gson = gsonBuilder.create();
    } else {
      gson = new GsonBuilder().excludeFieldsWithoutExposeAnnotation().create();
    }
    Writer writer = new BufferedWriter(new OutputStreamWriter(outputStream));

    gson.toJson(t, type, writer);
    writer.flush();
  }
}
