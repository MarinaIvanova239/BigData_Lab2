package mapreduce;

import org.apache.hadoop.io.Text;

public class DocumentInfo {

    Text fileName;
    int numberToken;
    int numberWords;
    double tf;
    double idf;

    DocumentInfo(Text fileName, int numberToken, int numberWords, double tf, double idf) {
        this.fileName = fileName;
        this.numberToken = numberToken;
        this.numberWords = numberWords;
        this.tf = tf;
        this.idf = idf;
    }

    Text getFileName() {
        return this.fileName;
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

    void setFileName(Text fileName) {
        this.fileName = fileName;
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
}
