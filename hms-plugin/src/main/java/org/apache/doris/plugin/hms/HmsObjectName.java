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

import java.util.Locale;
import java.util.Objects;

/** Construct only after resolving Doris names to remote HMS names. Catalog isolation is per instance. */
final class HmsObjectName {
    final String database;
    final String table;

    HmsObjectName(String database, String table) {
        this.database = Objects.requireNonNull(database, "remote database").toLowerCase(Locale.ROOT);
        this.table = Objects.requireNonNull(table, "remote table").toLowerCase(Locale.ROOT);
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof HmsObjectName)) {
            return false;
        }
        HmsObjectName that = (HmsObjectName) other;
        return database.equals(that.database) && table.equals(that.table);
    }

    @Override
    public int hashCode() {
        return Objects.hash(database, table);
    }

    @Override
    public String toString() {
        return database + "." + table;
    }
}
