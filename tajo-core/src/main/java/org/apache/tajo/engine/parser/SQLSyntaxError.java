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

package org.apache.tajo.engine.parser;


import org.apache.tajo.plan.InvalidQueryException;

public class SQLSyntaxError extends InvalidQueryException {
  private static final long serialVersionUID = 5388279335175632067L;

  private transient String detailedMessage;

  public SQLSyntaxError(String errorMessage) {
    super(errorMessage);
  }

  public SQLSyntaxError(SQLParseError e) {
    super(e.getMessageHeader(), e);
  }

  @Override
  public String getMessage() {
    if (detailedMessage == null) {
      if (getCause() != null) {
        detailedMessage = getCause().getMessage();
      } else {
        detailedMessage = String.format("ERROR: %s\n", super.getMessage());
      }
    }
    return detailedMessage;
  }
}
