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
import static org.apache.tajo.schema.IdentifierPolicy.ANSISQLPolicy;
import static org.apache.tajo.schema.QualifiedIdentifier.$;
import static org.apache.tajo.schema.TestTajoIdentifierPolicy.assertIdentifier;
import static org.apache.tajo.schema.TestTajoIdentifierPolicy.assertQualifiedIdentifier;

public class TestANSISQLIdentifierPolicy {

  @Test
  public void displayString() throws Exception {
    assertIdentifier(ANSISQLPolicy(), _("xyz"), "XYZ");
    assertIdentifier(ANSISQLPolicy(), _("XYZ"), "XYZ");

    assertIdentifier(ANSISQLPolicy(), _("xyz", true), "'xyz'");
    assertIdentifier(ANSISQLPolicy(), _("xYz", true), "'xYz'");
  }

  @Test
  public void testQualifiedIdentifiers() throws Exception {
    assertQualifiedIdentifier(ANSISQLPolicy(), $(_("xyz"), _("opq")), "XYZ.OPQ");
    assertQualifiedIdentifier(ANSISQLPolicy(), $(_("XYZ"), _("opq")), "XYZ.OPQ");

    assertQualifiedIdentifier(ANSISQLPolicy(), $(_("xyz", true), _("opq", false)), "'xyz'.OPQ");
    assertQualifiedIdentifier(ANSISQLPolicy(), $(_("xYz", true), _("opq", true)), "'xYz'.'opq'");
  }
}