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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/** Immutable snapshot, detached from mutable Hive Thrift response objects. */
final class HmsGrants {
    final HmsPrincipal owner;
    final Map<HmsPrincipal, Set<String>> privileges;

    HmsGrants(HmsPrincipal owner, Map<HmsPrincipal, Set<String>> privileges) {
        this.owner = owner;
        Map<HmsPrincipal, Set<String>> copy = new HashMap<>();
        privileges.forEach((principal, grants) -> copy.put(principal,
                Collections.unmodifiableSet(new HashSet<>(grants))));
        this.privileges = Collections.unmodifiableMap(copy);
    }

    boolean ownedBy(Set<HmsPrincipal> identities) {
        return identities.contains(owner);
    }

    boolean allows(Set<HmsPrincipal> identities, Set<String> required) {
        Set<String> granted = new HashSet<>();
        for (HmsPrincipal identity : identities) {
            granted.addAll(privileges.getOrDefault(identity, Collections.emptySet()));
        }
        return granted.containsAll(required);
    }
}
