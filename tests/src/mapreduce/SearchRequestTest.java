package mapreduce;

import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;

public class SearchRequestTest {

    @Test
    void checkFindingIntersection() {
        List<Map<String, Double>> data = new ArrayList<>();

        Map<String, Double> firstSet = new HashMap<>();
        firstSet.put("1.in", 0.5);
        firstSet.put("2.in", 0.6);
        firstSet.put("3.in", 0.9);

        Map<String, Double> secondSet = new HashMap<>();
        secondSet.put("3.in", 0.6);
        secondSet.put("alt.in", 0.6);
        secondSet.put("2.in", 0.12);

        data.add(firstSet);
        data.add(secondSet);

        List<Map.Entry<String, Double>> result = SearchRequest.findIntersection(data);

        List<Map.Entry<String, Double>> expected = new LinkedList<Map.Entry<String, Double>>();
        Map<String, Double> expectedSet = new HashMap<>();
        expectedSet.put("2.in", 0.6);
        expectedSet.put("3.in", 0.9);

        for(Map.Entry<String, Double> entry: expectedSet.entrySet()) {
            expected.add(entry);
        }

        assertEquals(expected, result);
    }

    @Test
    void checkReadingIndexFile() throws Exception {

        Map<String, Map<String, Double>> expected = new HashMap<>();

        Map<String, Double> documentsFirst = new HashMap<>();
        documentsFirst.put("1.in", 0.5);
        documentsFirst.put("2.in", 0.6);
        expected.put("Hello", documentsFirst);

        Map<String, Double> documentsSecond = new HashMap<>();
        documentsSecond.put("3.r", 0.4);
        documentsSecond.put("10zzz.p", 12.8999);
        documentsSecond.put("test.out", 12.0);
        expected.put("dear", documentsSecond);

        Map<String, Double> documentsThird = new HashMap<>();
        documentsThird.put("text.ru", 45.0);
        expected.put("friend", documentsThird);

        Map<String, Map<String, Double>> result = SearchRequest.readIndexFile("files/test_index.txt");

        assertEquals(expected, result);
    }

    @Test
    void checkReadingRequestFile() throws Exception {
        String expected = "Test string result";
        String result = SearchRequest.readRequestFile("files/test_request.txt");
        assertEquals(expected, result);
    }
}
