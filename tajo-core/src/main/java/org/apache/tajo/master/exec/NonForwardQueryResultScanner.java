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

package org.apache.tajo.master.exec;

import java.io.IOException;
import java.util.List;

import org.apache.tajo.QueryId;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableDesc;

import com.google.protobuf.ByteString;

public interface NonForwardQueryResultScanner {

  public void close() throws Exception;

  public Schema getLogicalSchema();

  public List<ByteString> getNextRows(int fetchRowNum) throws IOException;

  public QueryId getQueryId();
  
  public String getSessionId();
  
  public TableDesc getTableDesc();

  public void init() throws IOException;

  public int getCurrentRowNumber();

}
