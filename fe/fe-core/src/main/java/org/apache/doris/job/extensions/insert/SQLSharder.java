package org.apache.doris.job.extensions.insert;

import java.util.ArrayList;
import java.util.List;

public class SQLSharder {
    /**
     * Generate sharded SQLs based on the original SQL, shard key, shard count, lower and upper limit.
     * eg:originalSQL = "SELECT * FROM table WHERE age = 1", shardKey = "id", shardCount = 2, lowerLimit = 0, upperLimit = 100
     *    The result will be: ["SELECT * FROM table WHERE age = 1 AND id >= 0 AND id < 50", "SELECT * FROM table WHERE age = 1 AND id >= 50 AND id < 100"]
     *
     * @param originalSQL The original SQL to be sharded.
     * @param shardKey The shard key used for sharding.
     * @param shardCount The total number of shards.
     * @param lowerLimit The lower limit of the shard key.
     * @param upperLimit The upper limit of the shard key
     * @return A list of sharded SQLs.
     */
    public static List<String> generateShardedSQLs(String originalSQL, String shardKey, int shardCount, long lowerLimit, long upperLimit) {
        List<String> shardedSQLs = new ArrayList<>();
        long range = (upperLimit - lowerLimit) / shardCount;
        for (int i = 0; i < shardCount; i++) {
            long lowerRange = lowerLimit + i * range;
            long upperRange = i == shardCount - 1 ? upperLimit : lowerRange + range;
            String shardedSQL = originalSQL + (originalSQL.toLowerCase().contains("where") ? " AND " : " WHERE ")
                    + shardKey + " >= " + lowerRange + " AND " + shardKey + " < " + upperRange;
            shardedSQLs.add(shardedSQL);
        }
        return shardedSQLs;
    }
    
}
