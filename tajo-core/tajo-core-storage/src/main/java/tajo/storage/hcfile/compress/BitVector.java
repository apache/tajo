/*
 * Copyright 2012 Database Lab., Korea Univ.
 *
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

package tajo.storage.hcfile.compress;

import java.io.IOException;
import java.nio.ByteBuffer;

public class BitVector extends Codec {
  @Override
  public byte[] decompress(byte[] compressed) throws IOException {
    return new byte[0];  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public byte[] decompress(ByteBuffer compressed) throws IOException {
    return new byte[0];  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public byte[] compress(byte[] decompressed) throws IOException {
    return new byte[0];  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public byte[] compress(ByteBuffer decompressed) throws IOException {
    return new byte[0];  //To change body of implemented methods use File | Settings | File Templates.
  }
}
