package mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DocumentInfo implements Writable {

    long fileIndex;
    int numberToken;
    int numberWords;
    double tf;
    double idf;

    public DocumentInfo() {};

    DocumentInfo(long fileIndex, int numberToken, int numberWords, double tf, double idf) {
        this.fileIndex = fileIndex;
        this.numberToken = numberToken;
        this.numberWords = numberWords;
        this.tf = tf;
        this.idf = idf;
    }

    long getFileIndex() {
        return this.fileIndex;
    }

    int getNumberToken() {
        return this.numberToken;
    }

    int getNumberWords() {
        return this.numberWords;
    }

    double getTf() {
        return this.tf;
    }

    double getIdf() {
        return this.idf;
    }

    void setFileIndex(long fileIndex) {
        this.fileIndex = fileIndex;
    }

    void setNumberToken(int numberToken) {
        this.numberToken = numberToken;
    }

    void setNumberWords(int numberWords) {
        this.numberWords = numberWords;
    }

    void setTf(double tf) {
        this.tf = tf;
    }

    void setIdf(double idf) {
        this.idf = idf;
    }

    public void write(DataOutput out) throws IOException {
        out.writeLong(fileIndex);
        out.writeInt(numberToken);
        out.writeInt(numberWords);
        out.writeDouble(tf);
        out.writeDouble(idf);
    }

    public void readFields(DataInput in) throws IOException {
        fileIndex = in.readLong();
        numberToken = in.readInt();
        numberWords = in.readInt();
        tf = in.readDouble();
        idf = in.readDouble();
    }

    public String toString() {
        return Long.toString(fileIndex) + ", " +
                Integer.toString(numberToken) + ", " +
                Integer.toString(numberWords) + ", " +
                Double.toString(tf) + ", " +
                Double.toString(idf);
    }
}
