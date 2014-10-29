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

public class BooleanValidator extends AbstractValidator {

  @Override
  protected <T> String getErrorMessage(T object) {
    return object + " is not a valid boolean representation.";
  }

  @Override
  protected <T> boolean validateInternal(T object) {
    boolean result = false;
    
    if (object != null) {
      if (object instanceof Boolean) {
        result = true;
      } else if (object instanceof CharSequence) {
        String valueString = object.toString();
        if (Boolean.FALSE.toString().equalsIgnoreCase(valueString) || 
            Boolean.TRUE.toString().equalsIgnoreCase(valueString)) {
          result = true;
        }
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
