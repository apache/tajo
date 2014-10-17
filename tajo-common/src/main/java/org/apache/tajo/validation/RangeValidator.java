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

import java.util.Collection;
import java.util.Collections;

public class RangeValidator extends AbstractValidator {
  
  private final String minValue;
  private final AbstractValidator minValidator;
  private final String maxValue;
  private final AbstractValidator maxValidator;
  
  public RangeValidator(String minValue, String maxValue) {
    this.minValue = minValue;
    this.minValidator = new MinValidator(minValue);
    this.maxValue = maxValue;
    this.maxValidator = new MaxValidator(maxValue);
  }

  @Override
  protected <T> String getErrorMessage(T object) {
    return object + " is not a range of " + minValue + " and " + maxValue;
  }

  @Override
  protected <T> boolean validateInternal(T object) {
    return this.minValidator.validateInternal(object) & this.maxValidator.validateInternal(object);
  }

  @Override
  protected Collection<Validator> getDependantValidators() {
    return Collections.emptySet();
  }

}
