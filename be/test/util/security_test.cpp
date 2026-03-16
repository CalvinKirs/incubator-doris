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

#include <gtest/gtest.h>

#include <string>

#include "util/security.h"

namespace doris {

TEST(SecurityTest, MaskTokenMasksCredentialPairs) {
    std::string result = mask_token(
            "http://127.0.0.1/api/get_small_file?file_id=1&token=test-token&ak=test-ak&sk=test-sk"
            "&session_token=test-session");

    ASSERT_EQ(result.find("test-token"), std::string::npos);
    ASSERT_EQ(result.find("test-ak"), std::string::npos);
    ASSERT_EQ(result.find("test-sk"), std::string::npos);
    ASSERT_EQ(result.find("test-session"), std::string::npos);
    ASSERT_NE(result.find("token=******"), std::string::npos);
    ASSERT_NE(result.find("ak=******"), std::string::npos);
    ASSERT_NE(result.find("sk=******"), std::string::npos);
    ASSERT_NE(result.find("session_token=******"), std::string::npos);
    ASSERT_EQ(mask_token("raw-bearer-token"), "******");
}

} // namespace doris
