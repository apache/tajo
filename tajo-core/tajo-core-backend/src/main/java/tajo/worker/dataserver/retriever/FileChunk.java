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

package tajo.worker.dataserver.retriever;

import java.io.File;
import java.io.FileNotFoundException;

public class FileChunk {
  private final File file;
  private final long startOffset;
  private final long length;

  public FileChunk(File file, long startOffset, long length) throws FileNotFoundException {
    this.file = file;
    this.startOffset = startOffset;
    this.length = length;
  }

  public File getFile() {
    return this.file;
  }

  public long startOffset() {
    return this.startOffset;
  }

  public long length() {
    return this.length;
  }

  public String toString() {
    return " (start=" + startOffset() + ", length=" + length + ") "
        + file.getAbsolutePath();
  }
}
