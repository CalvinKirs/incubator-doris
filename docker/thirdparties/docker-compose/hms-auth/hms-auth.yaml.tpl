# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Host networking with loopback listeners: no published ports, nothing reachable from other machines.
services:
  hms-auth-hdfs:
    image: bde2020/hadoop-namenode:2.0.0-hadoop2.7.4-java8
    container_name: doris-${CONTAINER_UID}-hms-auth-hdfs
    network_mode: host
    entrypoint: ["bash", "/auth/scripts/start-hdfs.sh"]
    environment:
      HADOOP_CONF_DIR: /auth/conf
      HADOOP_HEAPSIZE: "512"
    volumes:
      - ./runtime/conf:/auth/conf:ro
      - ./scripts:/auth/scripts:ro
    mem_limit: 2g
    healthcheck:
      test: ["CMD-SHELL", "bash -c '</dev/tcp/${HMS_AUTH_HOST}/${HMS_AUTH_FS_PORT}' && hdfs dfsadmin -report 2>/dev/null | grep -q 'Live datanodes (1)'"]
      interval: 5s
      timeout: 20s
      retries: 60

  hms-auth-hms:
    build:
      context: .
      dockerfile: Dockerfile
      network: host
      args:
        HIVE_VERSION: "${HMS_AUTH_HIVE_VERSION}"
    image: doris-hms-auth:${HMS_AUTH_HIVE_VERSION}
    container_name: doris-${CONTAINER_UID}-hms-auth-hms
    network_mode: host
    entrypoint: ["bash", "/auth/scripts/start-hms.sh"]
    environment:
      HADOOP_CONF_DIR: /auth/conf
      HIVE_CONF_DIR: /auth/conf
      HADOOP_HEAPSIZE: "768"
      HMS_PORT: "${HMS_AUTH_HMS_PORT}"
    volumes:
      - ./runtime/conf:/auth/conf:ro
      - ./scripts:/auth/scripts:ro
    mem_limit: 1536m
    depends_on:
      hms-auth-hdfs:
        condition: service_healthy
    healthcheck:
      test: ["CMD-SHELL", "bash -c '</dev/tcp/${HMS_AUTH_HOST}/${HMS_AUTH_HMS_PORT}'"]
      interval: 5s
      timeout: 5s
      retries: 60

  hms-auth-hs2:
    image: doris-hms-auth:${HMS_AUTH_HIVE_VERSION}
    container_name: doris-${CONTAINER_UID}-hms-auth-hs2
    network_mode: host
    entrypoint: ["bash", "/auth/scripts/start-hs2.sh"]
    environment:
      HADOOP_CONF_DIR: /auth/conf
      HIVE_CONF_DIR: /auth/conf
      HADOOP_HEAPSIZE: "768"
      HMS_URI: "thrift://${HMS_AUTH_HOST}:${HMS_AUTH_HMS_PORT}"
      HS2_HOST: "${HMS_AUTH_HOST}"
      HS2_PORT: "${HMS_AUTH_HS2_PORT}"
    volumes:
      - ./runtime/conf:/auth/conf:ro
      - ./scripts:/auth/scripts:ro
    mem_limit: 1536m
    depends_on:
      hms-auth-hms:
        condition: service_healthy
    healthcheck:
      test: ["CMD-SHELL", "bash -c '</dev/tcp/${HMS_AUTH_HOST}/${HMS_AUTH_HS2_PORT}'"]
      interval: 5s
      timeout: 5s
      retries: 60

  hms-auth-kdc:
    build:
      context: .
      dockerfile: Dockerfile.kdc
      network: host
      args:
        - http_proxy
        - https_proxy
    image: doris-hms-auth-kdc:local
    container_name: doris-${CONTAINER_UID}-hms-auth-kdc
    network_mode: host
    environment:
      HMS_AUTH_REALM: "${HMS_AUTH_REALM}"
      FIXTURE_UID: "${HMS_AUTH_UID}"
      FIXTURE_GID: "${HMS_AUTH_GID}"
    volumes:
      - ./runtime/krb:/auth
      - ./scripts:/auth-scripts:ro
    healthcheck:
      test: ["CMD-SHELL", "test -f /auth/ready && bash -c '</dev/tcp/${HMS_AUTH_HOST}/${HMS_AUTH_KDC_PORT}'"]
      interval: 3s
      timeout: 5s
      retries: 40

  hms-auth-krb-hms:
    image: doris-hms-auth:${HMS_AUTH_HIVE_VERSION}
    container_name: doris-${CONTAINER_UID}-hms-auth-krb-hms
    network_mode: host
    entrypoint: ["bash", "/auth-scripts/start-hms-krb.sh"]
    environment:
      HADOOP_HEAPSIZE: "768"
      HMS_PORT: "${HMS_AUTH_KRB_HMS_PORT}"
    volumes:
      - ./runtime/krb:/auth:ro
      - ./scripts:/auth-scripts:ro
    mem_limit: 1536m
    depends_on:
      hms-auth-kdc:
        condition: service_healthy
      hms-auth-hdfs:
        condition: service_healthy
      hms-auth-hms:
        condition: service_started
    healthcheck:
      test: ["CMD-SHELL", "bash -c '</dev/tcp/${HMS_AUTH_HOST}/${HMS_AUTH_KRB_HMS_PORT}'"]
      interval: 5s
      timeout: 5s
      retries: 60
