package ragna.pipeline.common.model;

import java.util.Objects;

public class RawRecord {
    private String filename;
    private String line;

    public RawRecord() {
    }

    public RawRecord(String filename, String line) {
        this.filename = filename;
        this.line = line;
    }


    @Override
    public String toString() {
        return "RawRecord{" +
                "filename='" + filename + '\'' +
                ", line='" + line + '\'' +
                '}';
    }
    public String getFilename() {
        return filename;
    }

    public String getLine() {
        return line;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RawRecord rawRecord = (RawRecord) o;
        return Objects.equals(filename, rawRecord.filename) &&
                Objects.equals(line, rawRecord.line);
    }

    @Override
    public int hashCode() {
        return Objects.hash(filename, line);
    }
}
