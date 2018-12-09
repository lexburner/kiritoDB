package moe.cnkirito.kiritodb.index;

import com.carrotsearch.hppc.LongIntHashMap;

/**
 * the implementation of memory index using ${@link com.carrotsearch.hppc.LongIntHashMap}
 */
public class HppcMemoryIndex implements MemoryIndex {

    private LongIntHashMap indexMap;

    public HppcMemoryIndex() {
        this.indexMap = new LongIntHashMap();
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
    public int[] getOffset() {
        throw new UnsupportedOperationException("getOffset() unsupported");
    }
}
