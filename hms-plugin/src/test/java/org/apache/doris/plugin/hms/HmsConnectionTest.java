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

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.HiveObjectType;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import shade.doris.hive.org.apache.thrift.protocol.TBinaryProtocol;
import shade.doris.hive.org.apache.thrift.protocol.TCompactProtocol;
import shade.doris.hive.org.apache.thrift.protocol.TProtocolFactory;
import shade.doris.hive.org.apache.thrift.server.TServer;
import shade.doris.hive.org.apache.thrift.server.TSimpleServer;
import shade.doris.hive.org.apache.thrift.transport.TServerSocket;

import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

class HmsConnectionTest {
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void legacyRpcRoundTripOverHiveTransport(boolean compact) throws Exception {
        ThriftHiveMetastore.Iface handler = Mockito.mock(ThriftHiveMetastore.Iface.class);
        Database database = new Database();
        database.setName("sales");
        Table table = new Table();
        table.setDbName("sales");
        table.setTableName("orders");
        table.setOwner("alice");
        Mockito.when(handler.get_database("sales")).thenReturn(database);
        Mockito.when(handler.get_table("sales", "orders")).thenReturn(table);
        Mockito.when(handler.list_privileges(ArgumentMatchers.isNull(), ArgumentMatchers.isNull(),
                ArgumentMatchers.any(HiveObjectRef.class))).thenAnswer(invocation -> {
                    Assertions.assertFalse(((HiveObjectRef) invocation.getArgument(2)).isSetCatName());
                    return Collections.emptyList();
                });
        try (ServerSocket socket = new ServerSocket(0, 8, InetAddress.getLoopbackAddress())) {
            TServerSocket transport = new TServerSocket(socket);
            TProtocolFactory protocol = compact ? new TCompactProtocol.Factory() : new TBinaryProtocol.Factory();
            TServer server = new TSimpleServer(new TServer.Args(transport)
                    .processor(new ThriftHiveMetastore.Processor<>(handler)).protocolFactory(protocol));
            Thread serverThread = new Thread(server::serve, "hms-plugin-protocol-test");
            serverThread.setDaemon(true);
            serverThread.start();
            try {
                Map<String, String> properties = new HashMap<>();
                properties.put("hms.uri", "thrift://localhost:" + socket.getLocalPort());
                properties.put("cache.ttl.seconds", "0");
                properties.put("hive.metastore.execute.setugi", "false");
                properties.put("hive.metastore.thrift.compact.protocol.enabled", Boolean.toString(compact));
                HiveConf conf = new HmsPluginConfig(properties).hiveConf();
                try (HmsConnection connection = HmsConnection.open(conf)) {
                    Assertions.assertEquals("sales", connection.client.get_database("sales").getName());
                    Assertions.assertEquals("alice", connection.client.get_table("sales", "orders").getOwner());
                    HiveObjectRef object = new HiveObjectRef();
                    object.setObjectType(HiveObjectType.TABLE);
                    object.setDbName("sales");
                    object.setObjectName("orders");
                    Assertions.assertTrue(connection.client.list_privileges(null, null, object).isEmpty());
                }
                Mockito.verify(handler).get_table("sales", "orders");
                Mockito.verify(handler).get_database("sales");
            } finally {
                server.stop();
                transport.close();
                serverThread.join(5000);
                Assertions.assertFalse(serverThread.isAlive());
            }
        }
    }
}
