// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.plugin.hms;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

class HmsNameCaseTest {
    @Test
    void defaultLocaleCannotChangePrincipalOrObjectNormalization() {
        Locale original = Locale.getDefault();
        try {
            Locale.setDefault(Locale.forLanguageTag("tr-TR"));
            Assertions.assertEquals("billing_i", HmsPrincipal.role("BILLING_I").name);
            Assertions.assertEquals("I", HmsPrincipal.user("I").name);
            Assertions.assertNotEquals(HmsPrincipal.user("I"), HmsPrincipal.user("i"));
            Assertions.assertEquals(new HmsObjectName("billing", "items"),
                    new HmsObjectName("BILLING", "ITEMS"));
            Map<HmsPrincipal, String> facts = new HashMap<>();
            facts.put(HmsPrincipal.user("billing_i"), "user");
            facts.put(HmsPrincipal.role("BILLING_I"), "role");
            Assertions.assertEquals(2, facts.size());
            Assertions.assertEquals("user", facts.get(HmsPrincipal.user("billing_i")));
            Assertions.assertEquals("role", facts.get(HmsPrincipal.role("billing_i")));
        } finally {
            Locale.setDefault(original);
        }
    }
}
