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
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import shade.doris.hive.org.apache.thrift.protocol.TBinaryProtocol;
import shade.doris.hive.org.apache.thrift.protocol.TCompactProtocol;
import shade.doris.hive.org.apache.thrift.protocol.TProtocol;

/** Retain Hive's transport/SASL setup, but use the legacy HMS RPCs available in Hive 1.1. */
final class HmsConnection implements AutoCloseable {
    final ThriftHiveMetastore.Iface client;
    private final Runnable close;

    HmsConnection(ThriftHiveMetastore.Iface client, Runnable close) {
        this.client = client;
        this.close = close;
    }

    static HmsConnection open(HiveConf conf) throws Exception {
        HiveMetaStoreClient connection = new HiveMetaStoreClient(conf);
        try {
            TProtocol protocol = MetastoreConf.getBoolVar(conf, MetastoreConf.ConfVars.USE_THRIFT_COMPACT_PROTOCOL)
                    ? new TCompactProtocol(connection.getTTransport())
                    : new TBinaryProtocol(connection.getTTransport());
            return new HmsConnection(new ThriftHiveMetastore.Client(protocol), connection::close);
        } catch (RuntimeException e) {
            connection.close();
            throw e;
        }
    }

    @Override
    public void close() {
        close.run();
    }
}
