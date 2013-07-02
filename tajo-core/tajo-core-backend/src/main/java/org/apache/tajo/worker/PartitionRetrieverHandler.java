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

import org.apache.tajo.worker.dataserver.retriever.FileChunk;
import org.apache.tajo.worker.dataserver.retriever.RetrieverHandler;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class PartitionRetrieverHandler implements RetrieverHandler {
  private final String baseDir;

  public PartitionRetrieverHandler(String baseDir) {
    this.baseDir = baseDir;
  }

  @Override
  public FileChunk get(Map<String, List<String>> kvs) throws IOException {
    // nothing to verify the file because AdvancedDataRetriever checks
    // its validity of the file.
    File file = new File(baseDir + "/" + kvs.get("fn").get(0));

    return new FileChunk(file, 0, file.length());
  }
}
