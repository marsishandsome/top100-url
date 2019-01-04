package mars.mapreduce;

import java.io.IOException;

/**
 * Function used to process input data and called in map stage.
 */
public interface Mapper {
    KeyValue map(String line) throws IOException;
}
