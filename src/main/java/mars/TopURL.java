package mars;

import mars.mapreduce.Aggregator;
import mars.mapreduce.KeyValue;
import mars.mapreduce.Mapper;
import mars.mapreduce.Runner;
import mars.mapreduce.SingleThreadLocalRunner;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Calculate top100 URL
 */
public class TopURL {

    /**
     * Entry of command line
     *
     * @param args input file and output file
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        long start = System.currentTimeMillis();
        File inputFile = new File(args[0]);
        File outputFile = new File(args[1]);
        new TopURL().getTopK(100, inputFile, outputFile);
        long end = System.currentTimeMillis();
        System.out.println("total time: " + (end - start) / 1000 + "s");
    }

    /**
     * temporary directory to store shuffle files
     */
    private final static File TEMP_DIR = new File("data/tmp/");

    private final static Mapper MAPPER = line -> new KeyValue(line, 1L);
    private final static Aggregator AGGREGATOR = (key, v1, v2) -> v1 + v2;

    /**
     * Calculate top100 URL
     *
     * @param k
     * @param inputFile  the input file
     * @param outputFile the output file
     * @return the top k url and its occurrence number
     * @throws IOException
     */
    public Map<String, Long> getTopK(Integer k, File inputFile, File outputFile) throws IOException {
        Runner runner = new SingleThreadLocalRunner(k, inputFile, outputFile, MAPPER, AGGREGATOR, TEMP_DIR);
        return runner.getTopK();
    }

    /**
     * Calculate top100 URL, used by unit test
     *
     * @param k
     * @param intputData the input url data
     * @return the top k url and its occurrence number
     * @throws IOException
     */
    public Map<String, Long> getTopK(Integer k, List<String> intputData) throws IOException {
        Runner runner = new SingleThreadLocalRunner(k, intputData, MAPPER, AGGREGATOR, TEMP_DIR);
        return runner.getTopK();
    }
}
