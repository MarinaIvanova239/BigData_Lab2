package mapreduce;

import org.junit.Test;

import java.io.File;
import java.util.*;

import static org.junit.Assert.assertEquals;

public class SearchRequestTest {

    @Test
    public void checkFindingIntersection() {
        List<Map<Long, Double>> data = new ArrayList<>();

        Map<Long, Double> firstSet = new HashMap<>();
        firstSet.put(1L, 0.5);
        firstSet.put(2L, 0.6);
        firstSet.put(3L, 0.9);

        Map<Long, Double> secondSet = new HashMap<>();
        secondSet.put(3L, 0.6);
        secondSet.put(10L, 0.6);
        secondSet.put(2L, 0.12);

        data.add(firstSet);
        data.add(secondSet);

        List<Map.Entry<Long, Double>> result = SearchRequest.findIntersection(data);

        List<Map.Entry<Long, Double>> expected = new LinkedList<Map.Entry<Long, Double>>();
        Map<Long, Double> expectedSet = new HashMap<>();
        expectedSet.put(2L, 0.36);
        expectedSet.put(3L, 0.75);

        for(Map.Entry<Long, Double> entry: expectedSet.entrySet()) {
            expected.add(entry);
        }

        assertEquals(expected, result);
    }

    @Test
    public void checkReadingIndexFile() throws Exception {

        Map<String, Map<Long, Double>> expected = new HashMap<>();

        Map<Long, Double> documentsFirst = new HashMap<>();
        documentsFirst.put(1L, 0.5);
        documentsFirst.put(2L, 0.6);
        expected.put("Hello", documentsFirst);

        Map<Long, Double> documentsSecond = new HashMap<>();
        documentsSecond.put(3L, 0.4);
        documentsSecond.put(10L, 12.8999);
        documentsSecond.put(28L, 12.0);
        expected.put("dear", documentsSecond);

        Map<Long, Double> documentsThird = new HashMap<>();
        documentsThird.put(666L, 45.0);
        expected.put("friend", documentsThird);

        Map<String, Map<Long, Double>> result = SearchRequest.readIndexFile("files/test_index.txt");

        assertEquals(expected, result);
    }

    @Test
    public void checkReadingRequestFile() throws Exception {
        String expected = "Test string result";
        String result = SearchRequest.readRequestFile("files/test_request.txt");
        assertEquals(expected, result);
    }
}
