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

package org.apache.tajo.validation;

import org.apache.tajo.util.TUtil;

public class Validators {
  
  public static Validator groups(Validator...validators) {
    return new GroupValidator(TUtil.newHashSet(validators));
  }
  
  public static Validator length(int maxLength) {
    return new LengthValidator(maxLength);
  }
  
  public static Validator max(String maxValue) {
    return new MaxValidator(maxValue);
  }
  
  public static Validator min(String minValue) {
    return new MinValidator(minValue);
  }
  
  public static Validator notNull() {
    return new NotNullValidator();
  }
  
  public static Validator patternMatch(String regex) {
    return new PatternValidator(regex);
  }
  
  public static Validator range(String minValue, String maxValue) {
    return new RangeValidator(minValue, maxValue);
  }
  
  public static Validator pathUrl() {
    return new PathValidator();
  }

  public static Validator pathUrlList() {
    return new PathListValidator();
  }
  
  public static Validator shellVar() {
    return new ShellVariableValidator();
  }
  
  public static Validator networkAddr() {
    return new NetworkAddressValidator();
  }
  
  public static Validator bool() {
    return new BooleanValidator();
  }
  
  public static Validator clazz() {
    return new ClassValidator();
  }
  
  public static Validator javaString() {
    return new JavaStringValidator();
  }
  
}
