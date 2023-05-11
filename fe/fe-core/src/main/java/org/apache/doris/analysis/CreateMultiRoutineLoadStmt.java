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

package org.apache.doris.analysis;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.common.util.Util;
import org.apache.doris.load.RoutineLoadDesc;
import org.apache.doris.load.loadv2.LoadTask;
import org.apache.doris.load.routineload.RoutineLoadJob;
import org.apache.doris.qe.ConnectContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;

@Slf4j
public class CreateMultiRoutineLoadStmt extends DdlStmt {



    /*
    *     FROM [DATASOURCE_TYPE] datasource_properties(

    )
    WITH gloab_load_properties(
    // todo 新加源头格式 比如json或者binlog格式
     // JOB 全局配置
    )
    WITH DATASOURCE_MAPPING  DB_MAPPINGS (
          // 数据表结构等映射
          [DB_NAME](
                // 全局配置 比如TOPIC AND PARTITION 等 以及 TOPIC MAPPING TABLE RULE
                DB_LOAD_PROPERTIES(
                    TOPIC_MAPPING=([TOPIC_NAME] = [TABLE_NAME])
                    TOPIC_PARTITION_MAPPING="TOPIC_NAME=TOPIC_PARTITION"

                )
                [TABLE_NAME](
                      COLUMN_MAPPING=(COLUMN_NAME=TABLE_NAME.COLUMN_NAME;COLUMN_NAME+COLUMN_NAME=TABLE_NAME.COLUMN_NAME )
                      COLUMN_FILTER="WHERE ~ "
                      TOPIC_MAPPING=([TOPIC_NAME] = [TABLE_NAME])
                      TOPIC_PARTITION_MAPPING="TOPIC_NAME=TOPIC_PARTITION"
                 )
            )
    )
 */
    /* job global properties */
    public static final String JOB_LABEL_PREFIX_PROPERTY = "job_label_prefix";

    public static final String DESIRED_CONCURRENT_NUMBER_PROPERTY = "desired_concurrent_number";
    // max error number in ten thousand records
    public static final String MAX_ERROR_NUMBER_PROPERTY = "max_error_number";
    // the following 3 properties limit the time and batch size of a single routine load task
    public static final String MAX_BATCH_INTERVAL_SEC_PROPERTY = "max_batch_interval";
    public static final String MAX_BATCH_ROWS_PROPERTY = "max_batch_rows";
    public static final String MAX_BATCH_SIZE_PROPERTY = "max_batch_size";
    public static final String EXEC_MEM_LIMIT_PROPERTY = "exec_mem_limit";

    public static final String FORMAT = "format"; // the value is csv or json, default is csv
    public static final String STRIP_OUTER_ARRAY = "strip_outer_array";

    /* database config properties */
    public static final String DB_LOAD_PROPERTIES = "db_load_properties";

    public static final String TOPIC_MAPPING = "topic_mapping";

    public static final String TOPIC_PATTERN ="topic_pattern";

    public static final String TABLE_INCLUDE="table_include";

    public static final String TABLE_EXCLUDE="table_exclude";

    /**
     * table and topic mapping
     *  one by one eg: table_one:topic_one;table_two:topic_two
     *
     *
     */
    public static final String TABLE_TOPIC_MAPPING="table_topic_mapping";

    /* table config properties */

    /**
     * the value is append or merge or delete, default is append
     */
    public static final String MERGE_TYPE = "merge_type";

    public static final String JSON_TYPE = "json_type";

    /**
     * consider upstream data is not standard, we need transform it to standard data
     * eg debezium binlog data: {"before": {"id": 1, "name": "abc"}, "after": {"id": 1, "name": "def"}}
     * we can use json_format_rule to transform it to {"id": 1, "name": "def"} todo tobe confirm @CalvinKirs @Zhengyu
     */
    public static final String JSON_FORMAT_RULE = "json_format_rule";
    // load job global properties




    public static final String JSONPATHS = "jsonpaths";
    public static final String JSONROOT = "json_root";
    public static final String NUM_AS_STRING = "num_as_string";
    public static final String FUZZY_PARSE = "fuzzy_parse";

    public static final String KAFKA_TOPIC_PROPERTY = "kafka_topic";
    // optional
    public static final String KAFKA_PARTITIONS_PROPERTY = "kafka_partitions";
    public static final String KAFKA_OFFSETS_PROPERTY = "kafka_offsets";
    public static final String KAFKA_DEFAULT_OFFSETS = "kafka_default_offsets";
    public static final String KAFKA_ORIGIN_DEFAULT_OFFSETS = "kafka_origin_default_offsets";

    private static final String NAME_TYPE = "ROUTINE LOAD NAME";
    public static final String ENDPOINT_REGEX = "[-A-Za-z0-9+&@#/%?=~_|!:,.;]+[-A-Za-z0-9+&@#/%=~_|]";
    public static final String SEND_BATCH_PARALLELISM = "send_batch_parallelism";
    public static final String LOAD_TO_SINGLE_TABLET = "load_to_single_tablet";

    private static final ImmutableSet<String> PROPERTIES_SET = new ImmutableSet.Builder<String>()
            .add(DESIRED_CONCURRENT_NUMBER_PROPERTY)
            .add(MAX_ERROR_NUMBER_PROPERTY)
            .add(MAX_BATCH_INTERVAL_SEC_PROPERTY)
            .add(MAX_BATCH_ROWS_PROPERTY)
            .add(MAX_BATCH_SIZE_PROPERTY)
            .add(FORMAT)
            .add(JSONPATHS)
            .add(STRIP_OUTER_ARRAY)
            .add(NUM_AS_STRING)
            .add(FUZZY_PARSE)
            .add(JSONROOT)
            .add(LoadStmt.STRICT_MODE)
            .add(LoadStmt.TIMEZONE)
            .add(EXEC_MEM_LIMIT_PROPERTY)
            .add(SEND_BATCH_PARALLELISM)
            .add(LOAD_TO_SINGLE_TABLET)
            .build();

    // the following variables will be initialized after analyze
    // -1 as unset, the default value will set in RoutineLoadJob
    private String name;
    private String dbName;
    private RoutineLoadDesc routineLoadDesc;
    private int desiredConcurrentNum = 1;
    private long maxErrorNum = -1;
    private long maxBatchIntervalS = -1;
    private long maxBatchRows = -1;
    private long maxBatchSizeBytes = -1;
    private boolean strictMode = true;
    private long execMemLimit = 2 * 1024 * 1024 * 1024L;
    private String timezone = TimeUtils.DEFAULT_TIME_ZONE;
    private int sendBatchParallelism = 1;
    private boolean loadToSingleTablet = false;
    /**
     * RoutineLoad support json data.
     * Require Params:
     * 1) dataFormat = "json"
     * 2) jsonPaths = "$.XXX.xxx"
     */
    private String format = ""; //default is csv.
    private String jsonPaths = "";
    private String jsonRoot = ""; // MUST be a jsonpath string
    private boolean stripOuterArray = false;
    private boolean numAsString = false;
    private boolean fuzzyParse = false;

    private String comment = "";

    private LoadTask.MergeType mergeType;

    public static final Predicate<Long> DESIRED_CONCURRENT_NUMBER_PRED = (v) -> v > 0L;
    public static final Predicate<Long> MAX_ERROR_NUMBER_PRED = (v) -> v >= 0L;
    public static final Predicate<Long> MAX_BATCH_INTERVAL_PRED = (v) -> v >= 5 && v <= 60;
    public static final Predicate<Long> MAX_BATCH_ROWS_PRED = (v) -> v >= 200000;
    public static final Predicate<Long> MAX_BATCH_SIZE_PRED = (v) -> v >= 100 * 1024 * 1024 && v <= 1024 * 1024 * 1024;
    public static final Predicate<Long> EXEC_MEM_LIMIT_PRED = (v) -> v >= 0L;
    public static final Predicate<Long> SEND_BATCH_PARALLELISM_PRED = (v) -> v > 0L;

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);
        // check routine load name
        if (Strings.isNullOrEmpty(name)) {
            throw new AnalysisException("Routine load name is null or empty");
        }

        // check db name
        // check table name
    }

}
