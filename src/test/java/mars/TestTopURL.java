package mars;

import com.google.common.collect.ImmutableList;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertTrue;

public class TestTopURL {

    @Test
    public void testGetTop3() throws IOException {
        List<String> inputData = ImmutableList.of("333", "444", "111", "444", "222", "333", "444", "333", "222", "444");
        Integer k = 3;
        TopURL topURL = new TopURL();
        Map<String, Long> resultMap = topURL.getTopK(k, inputData);
        assertTrue(resultMap.size() == 3);
        assertTrue(resultMap.get("222") == 2L);
        assertTrue(resultMap.get("333") == 3L);
        assertTrue(resultMap.get("444") == 4L);
    }

    @Test
    public void testGetTop100() throws IOException {
        ArrayList<String> inputData = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            for (int j = i; j < 1000; j++) {
                inputData.add("url" + j);
            }
        }
        Collections.shuffle(inputData);

        Integer k = 100;
        TopURL topURL = new TopURL();
        Map<String, Long> resultMap = topURL.getTopK(k, inputData);
        assertTrue(resultMap.size() == 100);
        for (int i = 999; i >= 900; i--) {
            assertTrue(resultMap.get("url" + i) == i + 1);
        }
    }
}
