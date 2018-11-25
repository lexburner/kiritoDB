package moe.cnkirito.kiritodb.index;

import com.carrotsearch.hppc.LongIntHashMap;

public class HppcMemoryIndex implements MemoryIndex {

    private int indexSize;
    private LongIntHashMap indexMap;

    public HppcMemoryIndex() {
        this.indexSize = 0;
        this.indexMap = new LongIntHashMap();
    }

    @Override
    public void setSize(int size) {
        this.indexSize = size;
    }

    @Override
    public int getSize() {
        return indexMap.size();
    }

    @Override
    public void init() {
        //do nothing
    }

    @Override
    public void insertIndexCache(long key, int value) {
        this.indexMap.put(key, value);
    }

    @Override
    public int get(long key) {
        return this.indexMap.getOrDefault(key, -1);
    }

    @Override
    public long[] getKeys() {
        throw new UnsupportedOperationException("getKeys() unsupported");
    }

    @Override
    public int[] getOffsetInts() {
        throw new UnsupportedOperationException("getOffsetInts() unsupported");
    }
}
