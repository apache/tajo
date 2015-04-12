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

package org.apache.tajo.storage.orc;

import org.apache.hadoop.conf.Configuration;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.storage.FileScanner;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.fragment.Fragment;

import java.io.IOException;

public class OrcScanner extends FileScanner {

  public OrcScanner(Configuration conf, final Schema schema, final TableMeta meta, final Fragment fragment) {
    super(conf, schema, meta, fragment);
  }

  @Override
  public void init() throws IOException {
    if (targets == null) {
      targets = schema.toArray();
    }

    super.init();
  }

  @Override
  public Tuple next() throws IOException {
    return null;
  }

  @Override
  public void reset() throws IOException {

  }

  @Override
  public void close() throws IOException {

  }

  @Override
  public boolean isProjectable() {
    return false;
  }

  @Override
  public boolean isSelectable() {
    return false;
  }

  @Override
  public boolean isSplittable() {
    return false;
  }
}
