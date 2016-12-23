package mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.types.Pair;

import javax.print.Doc;
import java.util.List;

public class CustomComparison {

    static boolean listsArrayAreEqual(List<Pair<Text, TextArrayWritable>> first, List<Pair<Text, TextArrayWritable>> second) {
        if (first.size() != second.size())
            return false;

        for (int i = 0; i < first.size(); i++) {
            Pair<Text, TextArrayWritable> elemFirst = first.get(i);
            Pair<Text, TextArrayWritable> elemSecond = second.get(i);

            if (elemFirst.getFirst().compareTo(elemSecond.getFirst()) != 0)
                return false;
            if (elemFirst.getSecond().toString().compareTo(elemSecond.getSecond().toString()) != 0)
                return false;
        }

        return true;
    }

    static boolean listsDocsAreEqual(List<Pair<Text, DocumentInfo>> first, List<Pair<Text, DocumentInfo>> second) {
        if (first.size() != second.size())
            return false;

        for (int i = 0; i < first.size(); i++) {
            Pair<Text, DocumentInfo> elemFirst = first.get(i);
            Pair<Text, DocumentInfo> elemSecond = second.get(i);

            if (elemFirst.getFirst().compareTo(elemSecond.getFirst()) != 0)
                return false;
            if (elemFirst.getSecond().toString().compareTo(elemSecond.getSecond().toString()) != 0)
                return false;
        }

        return true;
    }
}
