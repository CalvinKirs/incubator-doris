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


import com.google.common.collect.Maps;
import org.apache.doris.common.Config;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * MySQL Protocol Handshake Response Packet.
 * Supports standard authentication, OIDC, and modern connection attributes.
 */
public class MysqlAuthPacket extends MysqlPacket {
    private int maxPacketSize;
    private int characterSet;
    private String userName;
    private byte[] authResponse;
    private String database;
    private String pluginName;
    private MysqlCapability capability;
    private Map<String, String> connectAttributes;
    private byte[] randomString;

    public String getUser() {
        return userName;
    }

    public byte[] getAuthResponse() {
        return authResponse;
    }

    public void setAuthResponse(byte[] bytes) {
        authResponse = bytes;
    }

    public String getDb() {
        return database;
    }

    public byte[] getRandomString() {
        return randomString;
    }

    public MysqlCapability getCapability() {
        return capability;
    }

    public String getPluginName() {
        return pluginName;
    }

    @Override
    public boolean readFrom(ByteBuffer buffer) {
        // 1. Read capability flags (4 bytes) - CLIENT_PROTOCOL_41 is mandatory
        int flags = MysqlProto.readInt4(buffer);
        capability = new MysqlCapability(flags);

        if (!capability.isProtocol41()) {
            return false;
        }

        // 2. Read basic connection parameters
        maxPacketSize = MysqlProto.readInt4(buffer);
        characterSet = MysqlProto.readInt1(buffer);

        // 3. Handle Reserved 23 bytes (Strict Protocol Alignment)
        // Standard MySQL protocol reserves 23 bytes here. 
        // We check for your specific proxy magic prefix.
        byte[] prefix = new byte[3];
        buffer.mark();
        buffer.get(prefix);
        if (new String(prefix).equals(Config.proxy_auth_magic_prefix)) {
            // If prefix matches, consume the 20-byte scramble/random string
            randomString = new byte[MysqlPassword.SCRAMBLE_LENGTH];
            buffer.get(randomString);
        } else {
            // Otherwise, strictly skip the 23 reserved bytes to align the pointer for userName
            buffer.reset();
            buffer.position(buffer.position() + 23);
        }

        // 4. Read User Name (Null-terminated string)
        userName = new String(MysqlProto.readNulTerminateString(buffer));

        // 5. Read Authentication Response (Password or OIDC Token)
        // This logic branches based on the authentication method negotiated
        if (capability.isPluginAuthDataLengthEncoded()) {
            // Required for OIDC and modern high-security plugins (Length-Encoded)
            authResponse = MysqlProto.readLenEncodedString(buffer);
        } else if (capability.isSecureConnection()) {
            // Standard 4.1 auth (e.g., mysql_native_password scramble response)
            int len = MysqlProto.readInt1(buffer);
            authResponse = MysqlProto.readFixedString(buffer, len);
        } else {
            // Legacy authentication (Null-terminated)
            authResponse = MysqlProto.readNulTerminateString(buffer);
        }

        // 6. Read Database Name if CLIENT_CONNECT_WITH_DB is set
        if (buffer.hasRemaining() && capability.isConnectedWithDb()) {
            database = new String(MysqlProto.readNulTerminateString(buffer));
        }

        // 7. Read Authentication Plugin Name if CLIENT_PLUGIN_AUTH is set
        // For OIDC, this will be "authentication_openid_connect_client"
        if (buffer.hasRemaining() && capability.isPluginAuth()) {
            pluginName = new String(MysqlProto.readNulTerminateString(buffer));
        }

        // 8. Read Connection Attributes if CLIENT_CONNECT_ATTRS is set
        // Crucial for modern drivers (JDBC/mysqlsh) which send telemetry data
        if (buffer.hasRemaining() && capability.isConnectAttrs()) {
            connectAttributes = Maps.newHashMap();
            long attrsLength = MysqlProto.readVInt(buffer);
            int startPos = buffer.position();

            // Loop until all attribute bytes are consumed
            while (buffer.position() - startPos < attrsLength) {
                String key = new String(MysqlProto.readLenEncodedString(buffer));
                String value = new String(MysqlProto.readLenEncodedString(buffer));
                connectAttributes.put(key, value);
            }
        }

        return true;
    }

    @Override
    public void writeTo(MysqlSerializer serializer) {
        // Implementation for serializing the packet back to the client if needed
    }
}