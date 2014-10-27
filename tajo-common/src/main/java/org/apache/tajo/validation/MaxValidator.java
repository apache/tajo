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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Collection;
import java.util.Collections;

import org.apache.commons.lang.math.NumberUtils;

public class MaxValidator extends AbstractValidator {
  
  private final String maxValue;
  
  public MaxValidator(String maxValue) {
    if (!NumberUtils.isNumber(maxValue)) {
      throw new IllegalArgumentException(maxValue + " is not a Java number.");
    }
    
    this.maxValue = maxValue;
  }

  @Override
  protected <T> String getErrorMessage(T object) {
    return object + " should be less than " + maxValue;
  }

  @Override
  protected <T> boolean validateInternal(T object) {
    boolean result = false;
    
    if (object != null) {
      if ((object instanceof Byte) || (object instanceof Short) || (object instanceof Integer)) {
        Integer objInteger = Integer.decode(object.toString());
        Integer maxInteger = Integer.decode(maxValue);
        result = objInteger.compareTo(maxInteger) <= 0;
      } else if (object instanceof Long) {
        Long objLong = Long.decode(object.toString());
        Long maxLong = Long.decode(maxValue);
        result = objLong.compareTo(maxLong) <= 0;
      } else if ((object instanceof Float) || (object instanceof Double)) {
        Double objDouble = Double.valueOf(object.toString());
        Double maxDouble = Double.valueOf(maxValue);
        result = objDouble.compareTo(maxDouble) <= 0;
      } else if (object instanceof BigInteger) {
        BigInteger objInteger = (BigInteger) object;
        BigInteger maxInteger = new BigInteger(maxValue);
        result = objInteger.compareTo(maxInteger) <= 0;
      } else if (object instanceof BigDecimal) {
        BigDecimal objDecimal = (BigDecimal) object;
        BigDecimal maxDecimal = new BigDecimal(maxValue);
        result = objDecimal.compareTo(maxDecimal) <= 0;
      }
    } else {
      result = true;
    }
    
    return result;
  }

  @Override
  protected Collection<Validator> getDependantValidators() {
    return Collections.emptySet();
  }

}
