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
import java.util.regex.Pattern;

public class NetworkAddressValidator extends AbstractValidator {
  
  private final Pattern hostnamePattern;
  private final Pattern portNumberPattern;
  
  public NetworkAddressValidator() {
    hostnamePattern = Pattern.compile(
        "^(?:[1-2]?[0-9]{1,2}.[1-2]?[0-9]{1,2}.[1-2]?[0-9]{1,2}.[1-2]?[0-9]{1,2}|[a-zA-Z][a-zA-Z0-9.-_]+)$");
    portNumberPattern = Pattern.compile("^[1-6]?[0-9]{1,4}$");
  }

  @Override
  protected <T> String getErrorMessage(T object) {
    return object + " is not a valid network address representation.";
  }

  @Override
  protected <T> boolean validateInternal(T object) {
    boolean result = false;
    
    if (object != null) {
      if (object instanceof CharSequence) {
        String valueString = object.toString();
        if (valueString.isEmpty()) {
          
        } else {
          int separatorIdx = valueString.indexOf(':');

          if (separatorIdx > -1) {
            result = (hostnamePattern.matcher(valueString.substring(0, separatorIdx)).find() & 
                portNumberPattern.matcher(valueString.substring(separatorIdx + 1)).find());
          } else {
            result = hostnamePattern.matcher(valueString).find();
          }
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
