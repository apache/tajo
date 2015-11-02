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

package org.apache.tajo.rule;

import java.util.HashMap;
import java.util.Map;

public class EvaluationContext {

  private final Map<String, Object> paramMap = new HashMap<>();
  
  public void clearParameters() {
    paramMap.clear();
  }
  
  public void addParameter(String name, Object value) {
    paramMap.put(name, value);
  }
  
  public void addParameters(Map<String, Object> params) {
    paramMap.putAll(params);
  }
  
  public Object getParameter(String name) {
    return getParameter(name, null);
  }
  
  public Object getParameter(String name, Object defaultValue) {
    Object returnValue = paramMap.get(name);
    
    if (returnValue == null) {
      returnValue = defaultValue;
    }
    
    return returnValue;
  }
  
}
