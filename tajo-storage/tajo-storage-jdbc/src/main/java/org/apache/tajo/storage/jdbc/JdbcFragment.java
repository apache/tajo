/*
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

package org.apache.tajo.storage.jdbc;

import org.apache.tajo.TajoConstants;
import org.apache.tajo.storage.fragment.BuiltinFragmentKinds;
import org.apache.tajo.storage.fragment.Fragment;

import java.net.URI;
import java.util.List;

/**
 * Fragment for the systems which connects to Tajo via the JDBC interface.
 */
public class JdbcFragment extends Fragment<Long> {

  // TODO: set start and end keys properly
  public JdbcFragment(String inputSourceId, URI uri) {
    super(BuiltinFragmentKinds.JDBC, uri, inputSourceId, null, null, TajoConstants.UNKNOWN_LENGTH, extractHosts(uri));
  }

  public JdbcFragment(String inputSourceId, URI uri, List<String> hostNames) {
    super(BuiltinFragmentKinds.JDBC, uri, inputSourceId, null, null, TajoConstants.UNKNOWN_LENGTH,
        hostNames.toArray(new String[hostNames.size()]));
  }

  private static String[] extractHosts(URI uri) {
    return new String[] {ConnectionInfo.fromURI(uri).host};
  }

  @Override
  public boolean isEmpty() {
    return false;
  }
}
