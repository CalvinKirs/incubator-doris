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

package org.apache.doris.service.arrowflight.tokens;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.mysql.privilege.Auth;
import org.apache.doris.service.arrowflight.auth2.FlightAuthResult;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

class FlightTokenManagerImplTest {
    private Env env;
    private Auth auth;
    private MockedStatic<Env> envMockedStatic;

    @BeforeEach
    void setUp() {
        env = Mockito.mock(Env.class);
        auth = Mockito.mock(Auth.class);
        envMockedStatic = Mockito.mockStatic(Env.class);
        envMockedStatic.when(Env::getCurrentEnv).thenReturn(env);
        Mockito.when(env.getAuth()).thenReturn(auth);
        Mockito.when(auth.getMaxConn("alice")).thenReturn(10L);
    }

    @AfterEach
    void tearDown() {
        if (envMockedStatic != null) {
            envMockedStatic.close();
        }
    }

    @Test
    void testValidateInvalidTokenDoesNotLeakBearerToken() throws Exception {
        try (FlightTokenManagerImpl manager = new FlightTokenManagerImpl(10, 1)) {
            IllegalArgumentException exception = Assertions.assertThrows(
                    IllegalArgumentException.class,
                    () -> manager.validateToken("leaked-token"));

            Assertions.assertTrue(exception.getMessage().contains("invalid bearer token"));
            Assertions.assertFalse(exception.getMessage().contains("leaked-token"));
        }
    }

    @Test
    void testInvalidateTokenDoesNotLeakBearerTokenInFollowupError() throws Exception {
        try (FlightTokenManagerImpl manager = new FlightTokenManagerImpl(10, 1)) {
            FlightTokenDetails tokenDetails = manager.createToken(
                    "alice",
                    FlightAuthResult.of("alice", new UserIdentity("alice", "%"), "127.0.0.1"));
            String token = tokenDetails.getToken();

            manager.invalidateToken(token);

            IllegalArgumentException exception = Assertions.assertThrows(
                    IllegalArgumentException.class,
                    () -> manager.validateToken(token));

            Assertions.assertTrue(exception.getMessage().contains("invalid bearer token"));
            Assertions.assertFalse(exception.getMessage().contains(token));
        }
    }
}
