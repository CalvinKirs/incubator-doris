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

suite("test_oidc_role_mapping_auth", "p0,auth") {
    String suiteName = "test_oidc_role_mapping_auth"
    String integrationName = "${suiteName}_oidc"
    String mappingName = "${suiteName}_mapping"
    String secondMappingName = "${suiteName}_mapping_second"
    String analystRole = "${suiteName}_analyst"
    String auditorRole = "${suiteName}_auditor"

    try_sql("DROP ROLE MAPPING IF EXISTS ${secondMappingName}")
    try_sql("DROP ROLE MAPPING IF EXISTS ${mappingName}")
    try_sql("DROP AUTHENTICATION INTEGRATION IF EXISTS ${integrationName}")
    try_sql("DROP ROLE IF EXISTS ${analystRole}")
    try_sql("DROP ROLE IF EXISTS ${auditorRole}")

    try {
        sql """CREATE ROLE ${analystRole}"""
        sql """CREATE ROLE ${auditorRole}"""

        sql """
            CREATE AUTHENTICATION INTEGRATION ${integrationName}
            PROPERTIES (
                'type'='oidc',
                'oidc.issuer'='https://issuer.example.com/realms/doris',
                'oidc.jwks_uri'='https://issuer.example.com/realms/doris/protocol/openid-connect/certs',
                'oidc.allowed_audiences'='doris'
            )
            COMMENT 'oidc integration for regression test'
        """

        def integrationRows = sql """
            SELECT
                NAME,
                TYPE,
                PROPERTIES,
                COMMENT
            FROM information_schema.authentication_integrations
            WHERE NAME = '${integrationName}'
            ORDER BY NAME
        """
        assertEquals(1, integrationRows.size())
        assertEquals(4, integrationRows[0].size())
        assertEquals(integrationName, integrationRows[0][0])
        assertEquals("oidc", integrationRows[0][1])
        assertTrue(integrationRows[0][2].contains(
                "\"oidc.issuer\" = \"https://issuer.example.com/realms/doris\""))
        assertTrue(integrationRows[0][2].contains(
                "\"oidc.jwks_uri\" = \"https://issuer.example.com/realms/doris/protocol/openid-connect/certs\""))
        assertTrue(integrationRows[0][2].contains("\"oidc.allowed_audiences\" = \"doris\""))
        assertEquals("oidc integration for regression test", integrationRows[0][3])

        sql """
            CREATE ROLE MAPPING ${mappingName}
            ON AUTHENTICATION INTEGRATION ${integrationName}
            RULE ( USING CEL 'has_group("analytics")' GRANT ROLE ${analystRole} )
            , RULE ( USING CEL 'has_scope("session:role:auditor")' GRANT ROLE ${auditorRole} )
            COMMENT 'oidc role mapping for regression test'
        """

        test {
            sql """
                CREATE ROLE MAPPING ${mappingName}
                ON AUTHENTICATION INTEGRATION ${integrationName}
                RULE ( USING CEL 'true' GRANT ROLE ${analystRole} )
            """
            exception "already exists"
        }

        test {
            sql """
                CREATE ROLE MAPPING ${secondMappingName}
                ON AUTHENTICATION INTEGRATION ${integrationName}
                RULE ( USING CEL 'true' GRANT ROLE ${analystRole} )
            """
            exception "already has a role mapping"
        }

        test {
            sql """DROP AUTHENTICATION INTEGRATION ${integrationName}"""
            exception "still has role mapping"
        }

        sql """DROP ROLE MAPPING ${mappingName}"""

        test {
            sql """DROP ROLE MAPPING ${mappingName}"""
            exception "does not exist"
        }

        sql """DROP AUTHENTICATION INTEGRATION ${integrationName}"""

        def droppedRows = sql """
            SELECT NAME
            FROM information_schema.authentication_integrations
            WHERE NAME = '${integrationName}'
            ORDER BY NAME
        """
        assertEquals(0, droppedRows.size())

        test {
            sql """DROP AUTHENTICATION INTEGRATION ${integrationName}"""
            exception "does not exist"
        }
    } finally {
        try_sql("DROP ROLE MAPPING IF EXISTS ${secondMappingName}")
        try_sql("DROP ROLE MAPPING IF EXISTS ${mappingName}")
        try_sql("DROP AUTHENTICATION INTEGRATION IF EXISTS ${integrationName}")
        try_sql("DROP ROLE IF EXISTS ${analystRole}")
        try_sql("DROP ROLE IF EXISTS ${auditorRole}")
    }
}
