package mars.mapreduce;

/**
 * key value pair
 */
public class KeyValue implements Comparable {
    private String key;
    private Long value;

    public KeyValue(String key, Long value) {
        this.key = key;
        this.value = value;
    }

    public String getKey() {
        return key;
    }

    public Long getValue() {
        return value;
    }

    @Override
    public int compareTo(Object o) {
        return value.compareTo(((KeyValue) o).getValue());
    }
}
