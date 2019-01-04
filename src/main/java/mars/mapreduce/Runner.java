package mars.mapreduce;

import java.io.IOException;
import java.util.Map;

/**
 * Interface for a simple map reduce framework.
 */
public interface Runner {

    /**
     *
     * @return top k occurrence items
     * @throws IOException
     */
    Map<String, Long> getTopK() throws IOException;
}
