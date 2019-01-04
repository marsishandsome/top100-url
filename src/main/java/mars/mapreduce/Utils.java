package mars.mapreduce;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.Map;

public final class Utils {

    public static final String ITEM_SEPERATOR = ",";
    public static final String LINE_SEPERATOR = "\n";

    private Utils() {
    }

    /**
     * write a map to a file
     *
     * @param file   the file to write
     * @param append if <code>true</code>, then bytes will be written
     *               to the end of the file rather than the beginning
     * @param map    the data to write
     * @throws IOException
     */
    public static void flushMap(File file, boolean append, Map<String, Long> map) throws IOException {
        FileOutputStream fos = new FileOutputStream(file, append);

        PrintWriter out = new PrintWriter(new OutputStreamWriter(fos));
        for (Map.Entry<String, Long> entry : map.entrySet()) {
            String url = entry.getKey();
            Long count = entry.getValue();
            out.print(url + ITEM_SEPERATOR + count + LINE_SEPERATOR);
        }
        out.flush();
        out.close();
    }

    /**
     * delete a directory
     *
     * @param dir the directory
     */
    public static void deleteDirectory(File dir) {
        if (!dir.isDirectory()) {
            return;
        }

        File[] files = dir.listFiles();
        for (File f : files) {
            if (f.isDirectory()) {
                deleteDirectory(f);
            } else {
                f.delete();
            }
        }

        dir.delete();
    }
}
