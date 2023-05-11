package org.apache.doris.analysis.multi.routineload;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.doris.analysis.CreateRoutineLoadStmt;
import org.apache.doris.common.AnalysisException;
import org.jetbrains.annotations.NotNull;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

public class KafkaDataSourceProperties extends MultiRoutineLoadDataSourceProperties{



    /**
     *  kafka broker list
     */
    public static final String KAFKA_BROKER_LIST_PROPERTY = "kafka_broker_list";

    private String kafkaBrokerList;

    /**
     * kafka topics, if this property is set, the kafka topics pattern property will be ignored
     * we can specify multiple topics, separated by comma
     * For example, if you have a set of topics named topic-1,topic-2,topic-3,topic-4,
     */
    public static final String KAFKA_TOPICS_PROPERTY = "kafka_topics";

    private String kafkaTopics;
    /**
     * kafka topics pattern, is a configuration parameter in Apache Kafka that allows you to specify a regular
     * expression pattern to match against the topic names
     * For example, if you have a set of topics named topic-1, topic-2, topic-3,topic-4,
     * KAFKA_TOPICS_PATTERN parameter is topic-,topic-*, then all topics will be matched
     */
    public static final String KAFKA_TOPICS_PATTERN_PROPERTY = "kafka_topics_pattern";

    /**
     * kafka group id
     */
    public static final String KAFKA_GROUP_ID_PROPERTY = "property.group.id";

    private String kafkaGroupId;

    public String KAFKA_GROUP_ID;
    public static final String KAFKA_CLIENT_ID_PROPERTY = "property_client_id";
    public static final String ENDPOINT_REGEX = "[-A-Za-z0-9+&@#/%?=~_|!:,.;]+[-A-Za-z0-9+&@#/%=~_|]";


    public static final String GLOBAL_TOPIC_PARTITION_PROPERTY = "global_topic_partition";

    private int globalTopicPartition;

    public static final String GLOBAL_TOPIC_OFFSET_PROPERTY = "global_topic_offset";

    private int globalTopicOffset;

    /**
     * custom topics partition
     *   eg: topic1:1,topic2:1,topic3:0:1:2:3
     */
    public static final String CUSTOM_TOPICS_PARTITION_PROPERTY = "custom_topics_partition";


    private Map<String, List<Integer>> customTopicsPartition;

    /**
     * custom topics offset
     *   eg: topic1:1,topic2:1,topic3:0:1:2:3
     */
    public static final String CUSTOM_TOPICS_OFFSET_PROPERTY = "custom_topics_offset";

    private Map<String, List<Integer>> customTopicsOffset;


    private  String kafkaClientId;
    private static final ImmutableSet<String> CONFIGURABLE_DATA_SOURCE_PROPERTIES_SET
            = new ImmutableSet.Builder<String>()
            .add(CreateRoutineLoadStmt.KAFKA_BROKER_LIST_PROPERTY)
            .add(CreateRoutineLoadStmt.KAFKA_TOPIC_PROPERTY)
            .add(CreateRoutineLoadStmt.KAFKA_PARTITIONS_PROPERTY)
            .add(CreateRoutineLoadStmt.KAFKA_OFFSETS_PROPERTY)
            .add(CreateRoutineLoadStmt.KAFKA_DEFAULT_OFFSETS)
            .build();

    public KafkaDataSourceProperties(Map<String, Object> properties, boolean isAlter) throws AnalysisException {
        super(properties, isAlter);
        if(CollectionUtils.isEmpty(Collections.singleton(properties))) {
           throw new IllegalArgumentException("properties is empty");
       }
    }

    @Override
    public boolean checkParameters() {

        return false;
    }
    @Override
    public String[] requiredProperties() {
        return new String[0];
    }

    /**
     * Topic - Partition - Offset
     *       - Partition - Offset
     *       - Partition - Offset
     *
     * @param properties
     * @throws AnalysisException
     */
    private void setKafkaTopicPartitions(Map<String,String> properties) throws AnalysisException {

        Map<String,Map<Integer,Long>> topicPartitions = Maps.newHashMap();
        globalTopicPartition = defaultPartition;
        globalTopicOffset = defaultOffset;
        if(StringUtils.isNotBlank(properties.get(GLOBAL_TOPIC_PARTITION_PROPERTY))) {
            globalTopicPartition= Integer.parseInt(properties.get(GLOBAL_TOPIC_PARTITION_PROPERTY));
        }
        if(StringUtils.isNotBlank(properties.get(GLOBAL_TOPIC_OFFSET_PROPERTY))) {
            globalTopicOffset = Integer.parseInt(properties.get(GLOBAL_TOPIC_OFFSET_PROPERTY));
        }
        /*Map<String,List<Integer>> customTopicsPartition = Maps.newHashMap();
        if(StringUtils.isNotBlank(properties.get(CUSTOM_TOPICS_PARTITION_PROPERTY))) {
            customTopicsPartition= parseCustomTopicsPartition(properties.get(CUSTOM_TOPICS_PARTITION_PROPERTY));
        }

        Map<String,List<Integer>> customTopicsOffset = Maps.newHashMap();
        if(StringUtils.isNotBlank(properties.get(CUSTOM_TOPICS_OFFSET_PROPERTY))) {
            customTopicsOffset = parseCustomTopicsPartition(properties.get(CUSTOM_TOPICS_OFFSET_PROPERTY));
        }*/

    }

    private Map<String,Map<Integer,Integer>> topicMappingMap(){

        Map<String,Map<Integer,Integer>> topicMap = Maps.newHashMap();
        topics.forEach(topic->{
            Map<Integer,Integer> partitionOffsetMap = Maps.newHashMap();
            if(customTopicsPartition.containsKey(topic)) {
                List<Integer> offsets = customTopicsPartition.get(topic);
                for(int i = 0; i < offsets.size(); i++) {
                    if(customTopicsOffset.containsKey(topic)) {
                        partitionOffsetMap.put(i,customTopicsOffset.get(topic).get(i));
                    } else {
                        partitionOffsetMap.put(i,globalTopicOffset);
                    }
                }
            } else {
                for(int i = 0; i < globalTopicPartition; i++) {
                    if(customTopicsOffset.containsKey(topic)) {
                        partitionOffsetMap.put(i,customTopicsOffset.get(topic).get(i));
                    } else {
                        partitionOffsetMap.put(i,globalTopicOffset);
                    }
                }
            }
            topicMap.put(topic,partitionOffsetMap);
        });
        return topicMap;
    }

    private Map<String,List<Integer>> getCustomTopicsOffset(String customTopicsOffset) {
        return getStringListMap(customTopicsOffset);
    }

    @NotNull
    private Map<String, List<Integer>> getStringListMap(String topicsParams) {
        Map<String,List<Integer>> topicMap = Maps.newHashMap();
        String[] topicsOffset = topicsParams.split(",");
        for(String topicOffset : topicsOffset) {
            String[] params = topicOffset.split(":");
            String topic = params[0];
            if(params.length == 1) {
                continue;
            }
            List<Integer> offsets = Lists.newArrayList();
            for(int i = 1; i < params.length; i++) {
                offsets.add(Integer.parseInt(params[i]));
            }
            topicMap.put(topic,offsets);
        }
        return topicMap;
    }

    private Map<String,List<Integer>> parseCustomTopicsPartition(String customTopicsPartition){
        return getStringListMap(customTopicsPartition);
    }
    private static final int defaultPartition = 1;
    private static final int defaultOffset = 0;

    private List<String> topics;
    private void checkKafkaBrokerList() throws AnalysisException {
        String[] kafkaBrokerList = this.kafkaBrokerList.split(",");
        for (String broker : kafkaBrokerList) {
            if (!Pattern.matches(CreateRoutineLoadStmt.ENDPOINT_REGEX, broker)) {
                throw new AnalysisException(KAFKA_BROKER_LIST_PROPERTY + ":" + broker
                        + " not match pattern " + ENDPOINT_REGEX);
            }
        }
    }
}
