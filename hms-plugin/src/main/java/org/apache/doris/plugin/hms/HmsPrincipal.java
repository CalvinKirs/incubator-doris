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

import org.apache.hadoop.hive.metastore.api.PrincipalType;

import java.util.Locale;
import java.util.Objects;

/** USER names are exact; only ROLE names are case insensitive. */
final class HmsPrincipal {
    final PrincipalType type;
    final String name;

    HmsPrincipal(PrincipalType type, String name) {
        this.type = Objects.requireNonNull(type, "principal type");
        Objects.requireNonNull(name, "principal name");
        this.name = type == PrincipalType.ROLE ? name.toLowerCase(Locale.ROOT) : name;
    }

    static HmsPrincipal user(String name) {
        return new HmsPrincipal(PrincipalType.USER, name);
    }

    static HmsPrincipal role(String name) {
        return new HmsPrincipal(PrincipalType.ROLE, name);
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof HmsPrincipal)) {
            return false;
        }
        HmsPrincipal that = (HmsPrincipal) other;
        return type == that.type && name.equals(that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, name);
    }
}
