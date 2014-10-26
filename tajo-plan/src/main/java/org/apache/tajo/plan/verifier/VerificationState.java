/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.plan.verifier;

import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.util.TUtil;

import java.util.List;

public class VerificationState {
  private static final Log LOG = LogFactory.getLog(VerificationState.class);
  List<String> errorMessages = Lists.newArrayList();

  public void addVerification(String error) {
    LOG.warn(TUtil.getCurrentCodePoint(1) + " causes: " + error);
    errorMessages.add(error);
  }

  public boolean verified() {
    return errorMessages.size() == 0;
  }

  public List<String> getErrorMessages() {
    return errorMessages;
  }
}
