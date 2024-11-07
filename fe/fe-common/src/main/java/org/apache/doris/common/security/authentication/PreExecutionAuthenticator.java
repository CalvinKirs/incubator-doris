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

package org.apache.doris.common.security.authentication;

import java.security.PrivilegedExceptionAction;
import java.util.concurrent.Callable;

public class PreExecutionAuthenticator {

    private HadoopAuthenticator hadoopAuthenticator;

    public PreExecutionAuthenticator(HadoopAuthenticator authenticator) {
        this.hadoopAuthenticator = authenticator;
    }

    public PreExecutionAuthenticator() {
    }

    public <T> T execute(Callable<T> task) throws Exception {
        if (hadoopAuthenticator != null) {
            PrivilegedExceptionAction<T> action = new CallableToPrivilegedExceptionActionAdapter<>(task);
            return hadoopAuthenticator.doAs(action);
        } else {
            return task.call();
        }
    }

    public HadoopAuthenticator getHadoopAuthenticator() {
        return hadoopAuthenticator;
    }

    public void setHadoopAuthenticator(HadoopAuthenticator hadoopAuthenticator) {
        this.hadoopAuthenticator = hadoopAuthenticator;
    }

    public class CallableToPrivilegedExceptionActionAdapter<T> implements PrivilegedExceptionAction<T> {
        private final Callable<T> callable;

        public CallableToPrivilegedExceptionActionAdapter(Callable<T> callable) {
            this.callable = callable;
        }

        @Override
        public T run() throws Exception {
            return callable.call();
        }
    }
}
