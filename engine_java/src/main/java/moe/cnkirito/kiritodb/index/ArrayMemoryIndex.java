package moe.cnkirito.kiritodb.index;

import java.util.Arrays;
import java.util.Comparator;

public class ArrayMemoryIndex implements MemoryIndex {

    // key 和文件逻辑偏移的映射
    private IndexEntry[] indexEntries;
    private int indexSize;

    public ArrayMemoryIndex() {
        this.indexEntries = new IndexEntry[CommitLogIndex.expectedNumPerPartition];
        this.indexSize = 0;
    }

    @Override
    public void setSize(int size) {
        this.indexSize = size;
    }

    @Override
    public void init() {
        this.sortAndCompact();
    }

    @Override
    public void insertIndexCache(long key, int value) {
        this.indexEntries[value] = new IndexEntry(key, value);
    }

    @Override
    public int get(long key) {
        return this.binarySearchPosition(key);
    }

    private void sortAndCompact() {
        Arrays.sort(indexEntries, 0, this.indexSize, (a, b) -> {
            if (a.getKey() == b.getKey()) {
                return Integer.compare(a.getOffsetInt(), b.getOffsetInt());
            } else {
                return Long.compare(a.getKey(), b.getKey());
            }
        });
//        IndexEntry[] newIndexEntries = new IndexEntry[252000 * 4];
//        newIndexEntries[0] = indexEntries[0];
//        int newIndexSize = 1;
//        for (int i = 1; i < this.indexSize; i++) {
//            if (indexEntries[i].getKey() != indexEntries[i - 1].getKey()) {
//                newIndexSize++;
//            }
//            newIndexEntries[newIndexSize - 1] = indexEntries[i];
//        }
//        this.indexEntries = newIndexEntries;
//        this.indexSize = newIndexSize;
    }

    private synchronized int binarySearchPosition(long key) {
        int index = Arrays.binarySearch(indexEntries, 0, indexSize, new IndexEntry(key,-1), Comparator.comparingLong(IndexEntry::getKey));
        if (index >= 0) {
            int resultIndex = index;
            for (int i = index + 1; i < indexSize; i++) {
                if (indexEntries[i].getKey() == key) {
                    resultIndex = i;
                } else {
                    break;
                }
            }
            return this.indexEntries[resultIndex].getOffsetInt();
        } else {
            return -1;
        }
    }

}
