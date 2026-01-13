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

package org.apache.doris.mysql;
import java.util.EnumSet;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * MySQL Protocol Capability Flags - Full Integrated Version
 * Contains all specific helper methods for OIDC and Plugin Authentication.
 */
public class MysqlCapability {

    public enum Flag {
        CLIENT_LONG_PASSWORD(0x00000001, "Support longer passwords"),
        CLIENT_FOUND_ROWS(0x00000002, "Return found rows instead of affected rows"),
        CLIENT_LONG_FLAG(0x00000004, "Get all column flags"),
        CLIENT_CONNECT_WITH_DB(0x00000008, "Database can be specified on connect"),
        CLIENT_NO_SCHEMA(0x00000010, "Don't allow database.table.column"),
        CLIENT_COMPRESS(0x00000020, "Can use compression protocol"),
        CLIENT_ODBC(0x00000040, "ODBC client"),
        CLIENT_LOCAL_FILES(0x00000080, "Can use LOAD DATA LOCAL"),
        CLIENT_IGNORE_SPACE(0x00000100, "Ignore spaces before '('"),
        CLIENT_PROTOCOL_41(0x00000200, "New 4.1 protocol"),
        CLIENT_INTERACTIVE(0x00000400, "Interactive client"),
        CLIENT_SSL(0x00000800, "Switch to SSL after handshake"),
        CLIENT_IGNORE_SIGPIPE(0x00001000, "Ignore sigpipes"),
        CLIENT_TRANSACTIONS(0x00002000, "Client knows about transactions"),
        CLIENT_RESERVED(0x00004000, "Old flag for 4.1 protocol"),
        CLIENT_SECURE_CONNECTION(0x00008000, "New 4.1 authentication"),
        CLIENT_MULTI_STATEMENTS(0x00010000, "Enable multi-statement support"),
        CLIENT_MULTI_RESULTS(0x00020000, "Enable multi-results support"),
        CLIENT_PS_MULTI_RESULTS(0x00040000, "Multi-results for prepared statements"),
        CLIENT_PLUGIN_AUTH(0x00080000, "Client supports plugin authentication"),
        CLIENT_CONNECT_ATTRS(0x00100000, "Client supports connection attributes"),
        CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA(0x00200000, "Length-encoded authentication data"),
        CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS(0x00400000, "Client handles expired passwords"),
        CLIENT_SESSION_TRACK(0x00800000, "Support session state tracking"),
        CLIENT_DEPRECATE_EOF(0x01000000, "Deprecate EOF packets in favor of OK packets");

        private final int flagBit;
        private final String description;

        Flag(int flagBit, String description) {
            this.flagBit = flagBit;
            this.description = description;
        }

        public int getFlagBit() { return flagBit; }
        public String getDescription() { return description; }
    }

    private final int flags;

    public MysqlCapability(int flags) {
        this.flags = flags;
    }

    // --- Predefined Capability Sets ---

    /**
     * Standard capability set for modern MySQL 8.0/9.0+ servers.
     */
    public static final int DEFAULT_FLAGS =
            Flag.CLIENT_PROTOCOL_41.getFlagBit()
                    | Flag.CLIENT_CONNECT_WITH_DB.getFlagBit()
                    | Flag.CLIENT_SECURE_CONNECTION.getFlagBit()
                    | Flag.CLIENT_PLUGIN_AUTH.getFlagBit()
                    | Flag.CLIENT_LONG_FLAG.getFlagBit()
                    | Flag.CLIENT_CONNECT_ATTRS.getFlagBit()
                    | Flag.CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA.getFlagBit();

    public static final MysqlCapability DEFAULT_CAPABILITY = new MysqlCapability(DEFAULT_FLAGS);

    /**
     * SSL-enabled capability set.
     */
    public static final MysqlCapability SSL_CAPABILITY = new MysqlCapability(
            DEFAULT_FLAGS | Flag.CLIENT_SSL.getFlagBit()
    );

    // --- Core Logic Methods ---

    public int getFlags() {
        return flags;
    }

    public boolean hasFlag(Flag flag) {
        return (flags & flag.getFlagBit()) != 0;
    }

    // --- Restored Specific Helper Methods ---

    public boolean isProtocol41() {
        return hasFlag(Flag.CLIENT_PROTOCOL_41);
    }

    public boolean isSSL() {
        return hasFlag(Flag.CLIENT_SSL);
    }

    public boolean isClientUseSsl() {
        return isSSL();
    }

    public boolean isTransactions() {
        return hasFlag(Flag.CLIENT_TRANSACTIONS);
    }

    public boolean isConnectedWithDb() {
        return hasFlag(Flag.CLIENT_CONNECT_WITH_DB);
    }

    public boolean isPluginAuth() {
        return hasFlag(Flag.CLIENT_PLUGIN_AUTH);
    }

    public boolean isSecureConnection() {
        return hasFlag(Flag.CLIENT_SECURE_CONNECTION);
    }

    public boolean isConnectAttrs() {
        return hasFlag(Flag.CLIENT_CONNECT_ATTRS);
    }

    /**
     * Critical for OIDC: Check if authentication data is length-encoded.
     */
    public boolean isPluginAuthDataLengthEncoded() {
        return hasFlag(Flag.CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA);
    }

    public boolean isSessionTrack() {
        return hasFlag(Flag.CLIENT_SESSION_TRACK);
    }

    public boolean isDeprecateEof() {
        return hasFlag(Flag.CLIENT_DEPRECATE_EOF);
    }

    public boolean isDeprecatedEOF() {
        return isDeprecateEof();
    }

    public boolean supportClientLocalFile() {
        return hasFlag(Flag.CLIENT_LOCAL_FILES);
    }

    public boolean isClientMultiStatements() {
        return hasFlag(Flag.CLIENT_MULTI_STATEMENTS);
    }

    // --- Object Methods ---

    @Override
    public String toString() {
        return EnumSet.allOf(Flag.class).stream()
                .filter(this::hasFlag)
                .map(Flag::name)
                .collect(Collectors.joining(" | "));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MysqlCapability that = (MysqlCapability) o;
        return flags == that.flags;
    }

    @Override
    public int hashCode() {
        return Objects.hash(flags);
    }
    /**
     * Validates if the client's capabilities meet the server's minimum requirements.
     * * @param server The capabilities defined/supported by the server.
     * @param client The capabilities reported by the client in Handshake Response.
     * @return true if the client is compatible with the server's mandatory requirements.
     */
    public static boolean isCompatible(MysqlCapability server, MysqlCapability client) {
        // 1. Mandatory: Protocol 41 is a must for modern MySQL (especially for OIDC)
        if (!client.isProtocol41()) {
            return false;
        }

        // 2. Mandatory: If server requires SSL, client must support it
        if (server.isSSL() && !client.isSSL()) {
            return false;
        }

        // 3. Mandatory: If server is configured for OIDC, client MUST support Plugin Auth
        if (server.isPluginAuth() && !client.isPluginAuth()) {
            return false;
        }

        return true;
    }
}