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

package org.apache.tajo.pullserver.retriever;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.TaskAttemptId;
import org.apache.tajo.TaskId;
import org.apache.tajo.pullserver.FileAccessForbiddenException;
import org.apache.tajo.util.TajoIdUtils;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.QueryStringDecoder;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class AdvancedDataRetriever implements DataRetriever {
  private static final Log LOG = LogFactory.getLog(AdvancedDataRetriever.class);
  private final Map<String, RetrieverHandler> handlerMap = Maps.newConcurrentMap();

  public AdvancedDataRetriever() {
  }
  
  public void register(String taskAttemptId, RetrieverHandler handler) {
    synchronized (handlerMap) {
      if (!handlerMap.containsKey(taskAttemptId)) {
        handlerMap.put(taskAttemptId, handler);
      }
    } 
  }
  
  public void unregister(String taskAttemptId) {
    synchronized (handlerMap) {
      if (handlerMap.containsKey(taskAttemptId)) {
        handlerMap.remove(taskAttemptId);
      }
    }
  }

  @Override
  public FileChunk [] handle(ChannelHandlerContext ctx, HttpRequest request)
      throws IOException {

    final Map<String, List<String>> params =
      new QueryStringDecoder(request.getUri()).parameters();

    if (!params.containsKey("qid")) {
      throw new FileNotFoundException("No such qid: " + params.containsKey("qid"));
    }

    if (params.containsKey("sid")) {
      List<FileChunk> chunks = Lists.newArrayList();
      List<String> taskIds = splitMaps(params.get("qid"));
      for (String eachTaskId : taskIds) {
        String[] taskIdSeqTokens = eachTaskId.split("_");
        ExecutionBlockId ebId = TajoIdUtils.createExecutionBlockId(params.get("sid").get(0));
        TaskId quid = new TaskId(ebId, Integer.parseInt(taskIdSeqTokens[0]));

        TaskAttemptId attemptId = new TaskAttemptId(quid, Integer.parseInt(taskIdSeqTokens[1]));

        RetrieverHandler handler = handlerMap.get(attemptId.toString());
        FileChunk chunk = handler.get(params);
        chunks.add(chunk);
      }
      return chunks.toArray(new FileChunk[chunks.size()]);
    } else {
      RetrieverHandler handler = handlerMap.get(params.get("qid").get(0));
      FileChunk chunk = handler.get(params);
      if (chunk == null) {
        if (params.containsKey("qid")) { // if there is no content corresponding to the query
          return null;
        } else { // if there is no
          throw new FileNotFoundException("No such a file corresponding to " + params.get("qid"));
        }
      }

      File file = chunk.getFile();
      if (file.isHidden() || !file.exists()) {
        throw new FileNotFoundException("No such file: " + file.getAbsolutePath());
      }
      if (!file.isFile()) {
        throw new FileAccessForbiddenException(file.getAbsolutePath() + " is not file");
      }

      return new FileChunk[] {chunk};
    }
  }

  private List<String> splitMaps(List<String> qids) {
    if (null == qids) {
      LOG.error("QueryId is EMPTY");
      return null;
    }

    final List<String> ret = new ArrayList<>();
    for (String qid : qids) {
      Collections.addAll(ret, qid.split(","));
    }
    return ret;
  }
}