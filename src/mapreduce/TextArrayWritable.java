package mapreduce;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class TextArrayWritable extends ArrayWritable {

    public TextArrayWritable() {
        super(Text.class);
    }

    public TextArrayWritable(Text[] values) {
        super(Text.class, values);
    }

    @Override
    public String toString() {
        Writable[] values = get();

        if (values == null)
            return "";

        String resultString = " : " + values[0].toString();
        for (int i = 1; i < values.length; i++) {
            resultString += " ; " + values[i].toString();
        }
        return resultString;
    }
}
