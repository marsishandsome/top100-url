package mars.mapreduce;

/**
 *  Function used to aggregate data.
 */
public interface Aggregator {
    Long aggregate(String key, Long v1, Long v2);
}
