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

package org.apache.doris.httpv2.meta;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.system.Frontend;
import org.apache.doris.system.SystemInfoService.HostInfo;

import jakarta.servlet.http.HttpServletRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;

/**
 * Unit tests for {@link MetaService#checkFromValidFe} and the node-ident token
 * emission in {@link org.apache.doris.common.util.HttpURLUtil}.
 *
 * <p>These cover the fe_meta_auth_token feature: the token check is additive on
 * top of the existing node-host check. An empty token keeps the legacy behavior,
 * so existing clusters and rolling upgrades are unaffected; a configured token
 * additionally requires callers to present a matching token header.
 */
class MetaServiceTest {

    private static final String CLIENT_HOST = "192.0.2.10";
    private static final String CLIENT_PORT = "9010";

    private Env env;
    private MockedStatic<Env> envMockedStatic;
    private String savedToken;

    @BeforeEach
    void setUp() {
        savedToken = Config.fe_meta_auth_token;
        env = Mockito.mock(Env.class);
        envMockedStatic = Mockito.mockStatic(Env.class);
        envMockedStatic.when(Env::getCurrentEnv).thenReturn(env);
        envMockedStatic.when(Env::getServingEnv).thenReturn(env);
        // MetaService's imageDir field is initialized from Env.getImageDir() at construction.
        Mockito.when(env.getImageDir()).thenReturn(System.getProperty("java.io.tmpdir"));
    }

    @AfterEach
    void tearDown() {
        Config.fe_meta_auth_token = savedToken;
        if (envMockedStatic != null) {
            envMockedStatic.close();
        }
    }

    private HttpServletRequest mockRequest(String host, String port, String token) {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        Mockito.when(request.getHeader(Env.CLIENT_NODE_HOST_KEY)).thenReturn(host);
        Mockito.when(request.getHeader(Env.CLIENT_NODE_PORT_KEY)).thenReturn(port);
        Mockito.when(request.getHeader(Env.FE_META_AUTH_TOKEN_KEY)).thenReturn(token);
        Mockito.when(request.getRemoteHost()).thenReturn(host);
        return request;
    }

    private void expectValidFe(boolean valid) {
        Frontend fe = valid ? new Frontend() : null;
        Mockito.when(env.checkFeExist(Mockito.anyString(), Mockito.anyInt())).thenReturn(fe);
    }

    private void invokeCheck(HttpServletRequest request) throws Exception {
        Method m = MetaService.class.getDeclaredMethod("checkFromValidFe", HttpServletRequest.class);
        m.setAccessible(true);
        try {
            m.invoke(new MetaService(), request);
        } catch (InvocationTargetException e) {
            // Unwrap so callers can assert on the real cause (InvalidClientException).
            if (e.getCause() instanceof RuntimeException) {
                throw (RuntimeException) e.getCause();
            }
            throw e;
        }
    }

    // Token empty (default): only the node-host check runs. A valid FE with no
    // token header must pass -- this is the rolling-upgrade / existing-cluster path.
    @Test
    void testLegacyNoTokenValidFePasses() throws Exception {
        Config.fe_meta_auth_token = "";
        expectValidFe(true);
        invokeCheck(mockRequest(CLIENT_HOST, CLIENT_PORT, null));
    }

    // The node-host check must remain: a request that is not from a known FE is
    // rejected regardless of token configuration.
    @Test
    void testInvalidFeRejectedWhenTokenEmpty() {
        Config.fe_meta_auth_token = "";
        expectValidFe(false);
        Assertions.assertThrows(InvalidClientException.class,
                () -> invokeCheck(mockRequest(CLIENT_HOST, CLIENT_PORT, null)));
    }

    // Token configured + matching header + valid FE -> pass.
    @Test
    void testTokenConfiguredMatchingPasses() throws Exception {
        Config.fe_meta_auth_token = "s3cr3t-token";
        expectValidFe(true);
        invokeCheck(mockRequest(CLIENT_HOST, CLIENT_PORT, "s3cr3t-token"));
    }

    // Token configured but request carries no token header -> reject, even though
    // the node-host check would pass. This is the security goal.
    @Test
    void testTokenConfiguredMissingHeaderRejected() {
        Config.fe_meta_auth_token = "s3cr3t-token";
        expectValidFe(true);
        Assertions.assertThrows(InvalidClientException.class,
                () -> invokeCheck(mockRequest(CLIENT_HOST, CLIENT_PORT, null)));
    }

    // Token configured but request carries a wrong token -> reject.
    @Test
    void testTokenConfiguredMismatchRejected() {
        Config.fe_meta_auth_token = "s3cr3t-token";
        expectValidFe(true);
        Assertions.assertThrows(InvalidClientException.class,
                () -> invokeCheck(mockRequest(CLIENT_HOST, CLIENT_PORT, "wrong-token")));
    }

    // Even with a matching token, an unknown FE host is still rejected: the token
    // check is additive, it does not replace the node-host check.
    @Test
    void testTokenMatchButInvalidFeStillRejected() {
        Config.fe_meta_auth_token = "s3cr3t-token";
        expectValidFe(false);
        Assertions.assertThrows(InvalidClientException.class,
                () -> invokeCheck(mockRequest(CLIENT_HOST, CLIENT_PORT, "s3cr3t-token")));
    }

    // Client side: the node-ident headers must include the token only when it is
    // configured, so that a default (empty) cluster emits exactly the legacy headers.
    @Test
    void testClientHeadersOmitTokenWhenEmpty() throws Exception {
        Config.fe_meta_auth_token = "";
        Mockito.when(env.getSelfNode()).thenReturn(new HostInfo("127.0.0.1", 9010));
        Map<String, String> headers = org.apache.doris.common.util.HttpURLUtil.getNodeIdentHeaders();
        Assertions.assertFalse(headers.containsKey(Env.FE_META_AUTH_TOKEN_KEY));
        Assertions.assertEquals("127.0.0.1", headers.get(Env.CLIENT_NODE_HOST_KEY));
    }

    @Test
    void testClientHeadersIncludeTokenWhenConfigured() throws Exception {
        Config.fe_meta_auth_token = "s3cr3t-token";
        Mockito.when(env.getSelfNode()).thenReturn(new HostInfo("127.0.0.1", 9010));
        Map<String, String> headers = org.apache.doris.common.util.HttpURLUtil.getNodeIdentHeaders();
        Assertions.assertEquals("s3cr3t-token", headers.get(Env.FE_META_AUTH_TOKEN_KEY));
    }
}
