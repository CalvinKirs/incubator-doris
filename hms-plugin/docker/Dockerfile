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

# Add the independent plugin to the reactor without changing fe/pom.xml.

# Java 8 and Hadoop 2.7.4; replace the base image's Hive with the target 1.1.0 distribution.
FROM bde2020/hive:2.3.2-postgresql-metastore
ADD apache-hive-1.1.0-bin.tar.gz /opt/
ENV HIVE_HOME=/opt/apache-hive-1.1.0-bin \
    HIVE_CONF_DIR=/opt/hms-auth-conf \
    HADOOP_CONF_DIR=/opt/hms-auth-conf
COPY hive-site.xml core-site.xml /opt/hms-auth-conf/
COPY start.sh /opt/hms-auth-start.sh
EXPOSE 9083
ENTRYPOINT ["bash", "/opt/hms-auth-start.sh"]
