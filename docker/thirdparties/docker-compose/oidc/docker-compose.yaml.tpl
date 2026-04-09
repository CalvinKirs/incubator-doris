#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

services:
  keycloak:
    image: ${KEYCLOAK_IMAGE}
    container_name: ${CONTAINER_UID}oidc-keycloak
    user: "1000"
    ports:
      - "${OIDC_HTTP_PORT}:8080"
    environment:
      - KC_HTTP_ENABLED=true
      - KC_HOSTNAME_STRICT=false
      - KC_BOOTSTRAP_ADMIN_USERNAME=${KEYCLOAK_ADMIN}
      - KC_BOOTSTRAP_ADMIN_PASSWORD=${KEYCLOAK_ADMIN_PASSWORD}
    command: ["start-dev", "--import-realm"]
    volumes:
      - ./realm/doris-realm.json:/opt/keycloak/data/import/doris-realm.json:ro
    healthcheck:
      test: ["CMD-SHELL", "bash -c ': >/dev/tcp/127.0.0.1/8080'"]
      interval: 10s
      timeout: 5s
      retries: 12
      start_period: 20s
    networks:
      - ${CONTAINER_UID}oidc

networks:
  ${CONTAINER_UID}oidc:
    name: ${CONTAINER_UID}oidc
