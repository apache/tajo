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

package org.apache.tajo.worker;

import org.apache.tajo.TajoProtos;
import org.apache.tajo.TajoProtos.FetcherState;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.pullserver.retriever.FileChunk;

import java.io.IOException;
import java.net.URI;
import java.util.List;

public abstract class AbstractFetcher {

  protected final URI uri;
  protected FileChunk fileChunk;
  protected final TajoConf conf;

  protected TajoProtos.FetcherState state;

  protected long startTime;
  protected volatile long finishTime;
  protected int fileNum;
  protected long fileLen;
  protected int messageReceiveCount;

  public AbstractFetcher(TajoConf conf, URI uri) {
    this(conf, uri, null);
  }

  public AbstractFetcher(TajoConf conf, URI uri, FileChunk fileChunk) {
    this.conf = conf;
    this.uri = uri;
    this.fileChunk = fileChunk;
    this.state = TajoProtos.FetcherState.FETCH_INIT;
  }

  public URI getURI() {
    return this.uri;
  }

  public long getStartTime() {
    return startTime;
  }

  public long getFinishTime() {
    return finishTime;
  }

  public long getFileLen() {
    return fileLen;
  }

  public int getFileNum() {
    return fileNum;
  }

  public TajoProtos.FetcherState getState() {
    return state;
  }

  public int getMessageReceiveCount() {
    return messageReceiveCount;
  }

  public abstract List<FileChunk> get() throws IOException;

  protected void endFetch(FetcherState state) {
    this.finishTime = System.currentTimeMillis();
    this.state = state;
  }
}
