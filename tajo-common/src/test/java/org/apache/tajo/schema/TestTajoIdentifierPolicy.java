/*
 * Lisensed to the Apache Software Foundation (ASF) under one
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

package org.apache.tajo.schema;

import org.junit.Test;

import static org.apache.tajo.schema.Identifier._;
import static org.apache.tajo.schema.IdentifierPolicy.DefaultPolicy;
import static org.apache.tajo.schema.QualifiedIdentifier.$;
import static org.junit.Assert.assertEquals;

public class TestTajoIdentifierPolicy {
  @Test
  public void testIdentifiers() throws Exception {
    assertIdentifier(DefaultPolicy(), _("xyz"), "xyz");
    assertIdentifier(DefaultPolicy(), _("XYZ"), "xyz");

    assertIdentifier(DefaultPolicy(), _("xyz", true), "'xyz'");
    assertIdentifier(DefaultPolicy(), _("xYz", true), "'xYz'");
  }

  @Test
  public void testQualifiedIdentifiers() throws Exception {
    assertQualifiedIdentifier(DefaultPolicy(), $(_("xyz"), _("opq")), "xyz.opq");
    assertQualifiedIdentifier(DefaultPolicy(), $(_("XYZ"), _("opq")), "xyz.opq");

    assertQualifiedIdentifier(DefaultPolicy(), $(_("xyz", true), _("opq", false)), "'xyz'.opq");
    assertQualifiedIdentifier(DefaultPolicy(), $(_("xYz", true), _("opq", true)), "'xYz'.'opq'");
  }

  public static void assertIdentifier(IdentifierPolicy p, Identifier id, String expected) {
    assertEquals(expected, id.raw(p));
  }

  public static void assertQualifiedIdentifier(IdentifierPolicy p, QualifiedIdentifier id, String expected) {
    assertEquals(expected, id.raw(p));
  }
}