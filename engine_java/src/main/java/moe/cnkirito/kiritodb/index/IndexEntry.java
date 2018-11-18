package moe.cnkirito.kiritodb.index;

import java.util.Objects;

public class IndexEntry {

    private long key;
    private int offsetInt;

    public IndexEntry() {
    }

    public IndexEntry(long key, int offsetInt) {
        this.key = key;
        this.offsetInt = offsetInt;
    }

    public long getKey() {
        return key;
    }

    public void setKey(long key) {
        this.key = key;
    }

    public int getOffsetInt() {
        return offsetInt;
    }

    public void setOffsetInt(int offsetInt) {
        this.offsetInt = offsetInt;
    }

}
