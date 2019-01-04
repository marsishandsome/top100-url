package mars;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.ArrayList;

/**
 * Generate test url data
 */
public class URLGenerator {

    /**
     * @param args output file, distinct url number, total url number, min url size, max url size
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        long start = System.currentTimeMillis();
        File outputFile = new File(args[0]);
        int distinctURLNumber = Integer.parseInt(args[1]);
        int totalURLNumber = Integer.parseInt(args[2]);
        int urlMinSize = Integer.parseInt(args[3]);
        int urlMaxSize = Integer.parseInt(args[4]);

        ArrayList<String> urls = new ArrayList<>(distinctURLNumber);
        for (int i = 0; i < distinctURLNumber; i++) {
            urls.add(getURL(urlMinSize, urlMaxSize));
        }

        outputFile.getParentFile().mkdirs();
        FileOutputStream fos = new FileOutputStream(outputFile, false);
        PrintWriter out = new PrintWriter(new OutputStreamWriter(fos));

        for (int i = 0; i < totalURLNumber; i++) {
            int index = getNum(0, distinctURLNumber - 1);
            String url = urls.get(index);
            out.print(url + "\n");
        }
        out.flush();
        out.close();
        long end = System.currentTimeMillis();
        System.out.println("total time: " + (end - start) / 1000 + "s");
    }

    private static final String BASE = "abcdefghijklmnopqrstuvwxyz0123456789";

    /**
     * url suffix
     */
    private static final String[] URL_SUFFIX = ".com,.cn,.gov,.edu,.net,.org,.int,.mil,.biz,.info".split(",");

    private static String getURL(int lMin, int lMax) {
        int length = getNum(lMin, lMax);
        StringBuffer sb = new StringBuffer();
        sb.append("http://");
        for (int i = 0; i < length; i++) {
            int number = (int) (Math.random() * BASE.length());
            sb.append(BASE.charAt(number));
        }
        sb.append(URL_SUFFIX[(int) (Math.random() * URL_SUFFIX.length)]);
        return sb.toString();
    }

    private static int getNum(int start, int end) {
        return (int) (Math.random() * (end - start + 1) + start);
    }
}
