package moe.cnkirito.kiritodb.index;

/**
 * save index in the memory
 */
public interface MemoryIndex {
    int getSize();
    void init();
    void insertIndexCache(long key, int value);
    int get(long key);
    long[] getKeys();
    int[] getOffset();
}
