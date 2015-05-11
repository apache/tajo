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

package org.apache.tajo.rpc;

import io.netty.channel.ChannelHandlerContext;

/**
 * Event listener for netty code. Users can subscribe events by using this interface.
 */
public interface ChannelEventListener {

  /**
   * Performs actions before channel open.
   * @param ctx
   */
  void channelRegistered(ChannelHandlerContext ctx);
  
  /**
   * Performs actions after channel close
   * @param ctx
   */
  void channelUnregistered(ChannelHandlerContext ctx);
}
