package moe.cnkirito.kiritodb.index;

public interface MemoryIndex {
    void setSize(int size);
    int getSize();
    void init();
    void insertIndexCache(long key, int value);
    int get(long key);
}
