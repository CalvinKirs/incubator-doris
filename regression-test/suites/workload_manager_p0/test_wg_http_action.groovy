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

import org.codehaus.groovy.runtime.IOGroovyMethods

suite("test_wg_http_action") {
    def fes = sql_return_maparray "show frontends"
    def fe = fes[0]

    String hostUrl = "http://${fe.Host}:${fe.HttpPort}/api/meta/namespaces/default_cluster/workloadgroups/root";
    String[] command = new String[]{"curl","-X", "GET", hostUrl, "-H", "Authorization: Basic cm9vdDo= "};

    logger.info(hostUrl)
    Process process = Runtime.getRuntime().exec(command);
    int code = process.waitFor()
    def err = IOGroovyMethods.getText(new BufferedReader(new InputStreamReader(process.getErrorStream())));
    def out = process.getText()

    logger.info("test_wg_http_action output: code=" + code + ", err=" + err + ", out=" + out)
    assertTrue(code == 0)
    assertTrue(out.contains("'root'@'%'") && out.contains("normal"))


}