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

package org.apache.tajo.engine.function.string;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HexStringConverter {
  private static HexStringConverter hexStringConverter = null;

  public static HexStringConverter getInstance() {
    if (hexStringConverter==null)
      hexStringConverter = new HexStringConverter();
    return hexStringConverter;
  }

  private HexStringConverter() {
  }

  public String encodeHex(String str) {
    StringBuffer buf = new StringBuffer();

    for(int i=0; i<str.length(); i++) {
      String tmp = Integer.toHexString(str.charAt(i));
      if(tmp.length() == 1)
        buf.append("0x0" + tmp);
      else
        buf.append("0x" + tmp);
    }

    return buf.toString();
  }

  public String decodeHex(String hexString) {
    Pattern p = Pattern.compile("(0x([a-fA-F0-9]{2}([a-fA-F0-9]{2})?))");
    Matcher m = p.matcher(hexString);

    StringBuffer buf = new StringBuffer();
    int hashCode = 0;
    while( m.find() ) {
      hashCode = Integer.decode("0x" + m.group(2));
      m.appendReplacement(buf, new String( Character.toChars(hashCode)));
    }

    m.appendTail(buf);

    return buf.toString();
  }
}
