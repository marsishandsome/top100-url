package mars.mapreduce;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Single thread implementation of the interface runner.
 */
public class SingleThreadLocalRunner implements Runner {

    /**
     * Number of shuffle partitions.
     * If input size is 100G, the average size of shuffle file will be 100M.
     */
    private static final int BUCKET_NUMBER = 1024;

    /**
     * The shuffle file will be cached in memory in a HashMap,
     * If the size of HashMap > BATCH_FLUSH_NUMBER,
     * the data will be flushed to disk.
     */
    private static final int BATCH_FLUSH_NUMBER = 100;

    private Integer k;
    private List<String> inputData;
    private File inputFile;
    private File outputFile;
    private Mapper mapper;
    private Aggregator aggregator;
    private File tmpDir;

    private File storageDir;

    public SingleThreadLocalRunner(Integer k, File inputFile, File outputFile, Mapper mapper, Aggregator aggregator, File tmpDir) {
        init(k, null, inputFile, outputFile, mapper, aggregator, tmpDir);
    }

    public SingleThreadLocalRunner(Integer k, List<String> inputData, Mapper mapper, Aggregator aggregator, File tmpDir) {
        init(k, inputData, null, null, mapper, aggregator, tmpDir);
    }

    private void init(Integer k, List<String> inputData, File inputFile, File outputFile, Mapper mapper, Aggregator aggregator, File tmpDir) {
        this.k = k;
        this.inputData = inputData;
        this.inputFile = inputFile;
        this.outputFile = outputFile;
        this.mapper = mapper;
        this.aggregator = aggregator;
        this.tmpDir = tmpDir;
    }

    @Override
    public Map<String, Long> getTopK() throws IOException {
        prepare();

        runMapper();

        return runReducer();
    }

    private void runMapper() throws IOException {
        HashMap<Integer, HashMap<String, Long>> map = new HashMap<>(BUCKET_NUMBER);

        if (this.inputFile != null) {
            BufferedReader reader = null;
            try {
                reader = new BufferedReader(new FileReader(inputFile));
                String line;
                while ((line = reader.readLine()) != null) {
                    KeyValue keyValue = this.mapper.map(line);
                    aggregateInMapper(keyValue, map);
                }
                reader.close();
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                if (reader != null) {
                    try {
                        reader.close();
                    } catch (IOException e1) {
                        e1.printStackTrace();
                    }
                }
            }
        } else {
            for (String line : this.inputData) {
                KeyValue keyValue = this.mapper.map(line);
                aggregateInMapper(keyValue, map);
            }
        }

        flushAll(map);
    }

    private void aggregateInMapper(KeyValue keyValue, HashMap<Integer, HashMap<String, Long>> map) throws IOException {
        Integer bucketNumber = bucketNumber(keyValue.getKey(), BUCKET_NUMBER);
        HashMap<String, Long> bucketMap = map.get(bucketNumber);
        if (bucketMap == null) {
            bucketMap = new HashMap<>(BATCH_FLUSH_NUMBER);
            map.put(bucketNumber, bucketMap);
        }

        Long oldValue = bucketMap.getOrDefault(keyValue.getKey(), 0L);
        Long newValue = this.aggregator.aggregate(keyValue.getKey(), oldValue, keyValue.getValue());
        bucketMap.put(keyValue.getKey(), newValue);

        if (bucketMap.size() >= BATCH_FLUSH_NUMBER) {
            flush(bucketNumber, bucketMap);
            map.remove(bucketNumber);
        }
    }

    private void flushAll(HashMap<Integer, HashMap<String, Long>> map) throws IOException {
        for (Map.Entry<Integer, HashMap<String, Long>> engty : map.entrySet()) {
            Integer bucketNumber = engty.getKey();
            HashMap<String, Long> bucketMap = engty.getValue();
            flush(bucketNumber, bucketMap);
        }
        map.clear();
    }

    private void flush(Integer bucketNumber, HashMap<String, Long> bucketMap) throws IOException {
        File file = new File(this.storageDir, bucketNumber.toString());
        Utils.flushMap(file, true, bucketMap);
    }

    private Map<String, Long> runReducer() throws IOException {
        File[] files = this.storageDir.listFiles();
        if (files == null) {
            return Collections.emptyMap();
        }

        MinHeap minHeap = new MinHeap(k);
        for (File file : files) {
            HashMap<String, Long> map = mergeFile(file);
            for (Map.Entry<String, Long> entry : map.entrySet()) {
                minHeap.add(new KeyValue(entry.getKey(), entry.getValue()));
            }
        }

        List<KeyValue> resultList = minHeap.getAll();
        HashMap<String, Long> resultMap = new HashMap<>(k);
        resultList.stream().forEach(keyValue -> resultMap.put(keyValue.getKey(), keyValue.getValue()));
        if (this.outputFile != null) {
            Utils.flushMap(this.outputFile, false, resultMap);
        }
        return resultMap;
    }

    private HashMap<String, Long> mergeFile(File file) {
        HashMap<String, Long> map = new HashMap<>(k);
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(file));
            String line;
            while ((line = reader.readLine()) != null) {
                aggregateInReduce(line, map);
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
            }
        }
        return map;
    }

    private void aggregateInReduce(String line, HashMap<String, Long> map) {
        String[] parts = line.split(Utils.ITEM_SEPERATOR);
        String key = parts[0];
        Long value = Long.parseLong(parts[1]);

        Long oldValue = map.getOrDefault(key, 0L);
        Long newValue = this.aggregator.aggregate(key, oldValue, value);
        map.put(key, newValue);
    }


    private void prepare() {
        while (true) {
            UUID uuid = UUID.randomUUID();
            this.storageDir = new File(tmpDir, uuid.toString());
            if (!this.storageDir.exists()) {
                this.storageDir.mkdirs();
                break;
            }

        }
    }

    private Integer bucketNumber(String url, Integer bucketNumber) {
        return (url.hashCode() & 0x7FFFFFFF) % bucketNumber;
    }
}
