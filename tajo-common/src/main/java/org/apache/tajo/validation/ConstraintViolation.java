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

public class ConstraintViolation {

  private String message;
  private Class<? extends Validator> validatorClazz;

  public String getMessage() {
    return message;
  }

  public void setMessage(String message) {
    this.message = message;
  }

  public Class<? extends Validator> getValidatorClazz() {
    return validatorClazz;
  }

  public void setValidatorClazz(Class<? extends Validator> validatorClazz) {
    this.validatorClazz = validatorClazz;
  }

  @Override
  public String toString() {
    return "ConstraintViolation [message=" + message + ", validatorClazz=" + validatorClazz + "]";
  }
  
}
