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

import org.apache.http.conn.util.InetAddressUtils;

public class NetworkAddressValidator extends AbstractValidator {
  
  private final Pattern hostnamePattern;
  private final Pattern portNumberPattern;
  
  public NetworkAddressValidator() {
    hostnamePattern = Pattern.compile(
        "^\\w[\\w-]*(\\.\\w[\\w-]*)*\\.[a-zA-Z][\\w-]*.?$|[a-zA-Z][\\w-]*.?$");
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
          result = true;
        } else {
          int separatorIdx = valueString.indexOf(':');
          String hostOrIpAddress = null;
          
          if (separatorIdx > -1) {
            if (valueString.indexOf(':', separatorIdx+1) > -1) {
              // it is IPV6 representation.
              int leftBracketsIdx = valueString.indexOf('[');
              int rightBracketsIdx = valueString.indexOf(']');
              int periodIdx = valueString.indexOf('.');
              
              if ((leftBracketsIdx > -1) && (rightBracketsIdx > -1) && 
                  (valueString.length() > (rightBracketsIdx+1)) &&
                  valueString.charAt(rightBracketsIdx+1) == ':') {
                hostOrIpAddress = valueString.substring(leftBracketsIdx+1, rightBracketsIdx);
                separatorIdx = rightBracketsIdx+1;
              } else if ((periodIdx > -1)) {
                hostOrIpAddress = valueString.substring(0, periodIdx);
                separatorIdx = periodIdx;
              } else {
                separatorIdx = valueString.lastIndexOf(':');
                hostOrIpAddress = valueString.substring(0, separatorIdx);
              }
            } else {            
              hostOrIpAddress = valueString.substring(0, separatorIdx);
            }
          } else {
            hostOrIpAddress = valueString;
          }
          
          result = ((hostnamePattern.matcher(hostOrIpAddress).find()) | 
              InetAddressUtils.isIPv4Address(hostOrIpAddress) |
              InetAddressUtils.isIPv6Address(hostOrIpAddress));
          
          if (separatorIdx > -1) {
            result &= portNumberPattern.matcher(valueString.substring(separatorIdx + 1)).find();
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
