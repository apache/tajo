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

package org.apache.tajo.pullserver.listener;

import org.apache.hadoop.mapred.FadvisedFileRegion;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.apache.tajo.pullserver.TajoPullServerService;

public class FileCloseListener implements ChannelFutureListener {

  private FadvisedFileRegion filePart;
  private String requestUri;
  private TajoPullServerService pullServerService;
  private long startTime;

  public FileCloseListener(FadvisedFileRegion filePart,
                           String requestUri,
                           long startTime,
                           TajoPullServerService pullServerService) {
    this.filePart = filePart;
    this.requestUri = requestUri;
    this.pullServerService = pullServerService;
    this.startTime = startTime;
  }

  // TODO error handling; distinguish IO/connection failures,
  //      attribute to appropriate spill output
  @Override
  public void operationComplete(ChannelFuture future) {
    if(future.isSuccess()){
      filePart.transferSuccessful();
    }
    filePart.releaseExternalResources();
    if (pullServerService != null) {
      pullServerService.completeFileChunk(filePart, requestUri, startTime);
    }
  }
}
