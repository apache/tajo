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

package org.apache.tajo.datum.protobuf;

import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class TestTextUtils {

  @Test
  public void testDigitValueSuccess() {
      assertEquals(TextUtils.digitValue('0'), 0);
      assertEquals(TextUtils.digitValue('a'), 10);
      assertEquals(TextUtils.digitValue('A'), 10);
      assertEquals(TextUtils.digitValue('f'), 15);
      assertEquals(TextUtils.digitValue('F'), 15);
      assertEquals(TextUtils.digitValue('z'), 35);
      assertEquals(TextUtils.digitValue('Z'), 35);
  }

  @Test
  public void testDigitValueError() throws IllegalArgumentException {
      boolean ret = true;
      try {
        TextUtils.digitValue('!');
      } catch(IllegalArgumentException e) {
        ret = false;
      }

      assertFalse(ret);
  }
}
