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

/**
 * 
 */
package org.apache.tajo.worker;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.apache.tajo.TaskId;
import org.apache.tajo.worker.dataserver.FileAccessForbiddenException;
import org.apache.tajo.worker.dataserver.retriever.DataRetriever;
import org.apache.tajo.worker.dataserver.retriever.FileChunk;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;
import java.util.Set;

@Deprecated
public class InterDataRetriever implements DataRetriever {
  private final Log LOG = LogFactory.getLog(InterDataRetriever.class);
  private final Set<TaskId> registered = Sets.newHashSet();
  private final Map<String, String> map = Maps.newConcurrentMap();

  public InterDataRetriever() {
  }
  
  public void register(TaskId id, String baseURI) {
    synchronized (registered) {
      if (!registered.contains(id)) {      
        map.put(id.toString(), baseURI);
        registered.add(id);      
      }
    } 
  }
  
  public void unregister(TaskId id) {
    synchronized (registered) {
      if (registered.contains(id)) {
        map.remove(id.toString());
        registered.remove(id);
      }
    }
  }

  /* (non-Javadoc)
   * @see org.apache.tajo.worker.dataserver.retriever.DataRetriever#handle(org.jboss.netty.channel.ChannelHandlerContext, org.jboss.netty.handler.codec.http.HttpRequest)
   */
  @Override
  public FileChunk [] handle(ChannelHandlerContext ctx, HttpRequest request)
      throws IOException {
       
    int start = request.getUri().indexOf('?');
    if (start < 0) {
      throw new IllegalArgumentException("Wrong request: " + request.getUri());
    }
    
    String queryStr = request.getUri().substring(start + 1);
    LOG.info("QUERY: " + queryStr);
    String [] queries = queryStr.split("&");
    
    String qid = null;
    String fn = null;
    String [] kv;
    for (String query : queries) {
      kv = query.split("=");
      if (kv[0].equals("qid")) {
        qid = kv[1];
      } else if (kv[0].equals("fn")) {
        fn = kv[1];
      }
    }
    
    String baseDir = map.get(qid);
    if (baseDir == null) {
      throw new FileNotFoundException("No such qid: " + qid);
    }

    File file = new File(baseDir + "/" + fn);
    if (file.isHidden() || !file.exists()) {
      throw new FileNotFoundException("No such file: " + baseDir + "/" 
          + file.getName());
    }
    if (!file.isFile()) {
      throw new FileAccessForbiddenException("No such file: " 
          + baseDir + "/" + file.getName()); 
    }
    
    return new FileChunk[] {new FileChunk(file, 0, file.length())};
  }
}