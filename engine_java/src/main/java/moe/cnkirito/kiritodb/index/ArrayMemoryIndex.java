package moe.cnkirito.kiritodb.index;

public class ArrayMemoryIndex implements MemoryIndex {

    // keys 和文件逻辑偏移的映射
//    private IndexEntry[] indexEntries;
    private long keys[];
    private int offsetInts[];
    private int indexSize;

    public ArrayMemoryIndex() {
//        this.indexEntries = new IndexEntry[CommitLogIndex.expectedNumPerPartition];
        this.keys = new long[CommitLogIndex.expectedNumPerPartition];
        this.offsetInts = new int[CommitLogIndex.expectedNumPerPartition];
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
        this.keys[value] = key;
        this.offsetInts[value] = value;
//        this.indexEntries[value] = new IndexEntry(keys, value);
    }

    @Override
    public int get(long key) {
        return this.binarySearchPosition(key);
    }

    private void sortAndCompact() {
        sort(0, this.indexSize - 1);
        compact();
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

    private void compact() {
        long[] newKeys = new long[CommitLogIndex.expectedNumPerPartition];
        int[] newOffsetInts = new int[CommitLogIndex.expectedNumPerPartition];

        int curIndex = 0;
        newOffsetInts[0] = this.offsetInts[0];
        newKeys[0] = this.keys[0];
        for (int i = 1; i < this.indexSize; i++) {
            if (this.keys[i] != this.keys[i - 1]) {
                curIndex++;
                newKeys[curIndex] = this.keys[i];
                newOffsetInts[curIndex] = this.offsetInts[i];
            } else {
                newOffsetInts[curIndex] = Math.max(newOffsetInts[i], this.offsetInts[i]);
            }
        }
        this.indexSize = curIndex + 1;
        this.offsetInts = newOffsetInts;
        this.keys = newKeys;
    }

    private int binarySearchPosition(long key) {
        int index = this.binarySearch(0, indexSize, key);
        if (index >= 0) {
            return this.offsetInts[index];
        } else {
            return -1;
        }
    }

    public void sort(int low, int high) {
        int start = low;
        int end = high;
        long key = this.keys[low];

        while (end > start) {
            while (end > start && this.keys[end] >= key)
                end--;
            if (this.keys[end] <= key) {
                swap(start, end);
            }
            //从前往后比较
            while (end > start && this.keys[start] <= key)
                start++;
            if (this.keys[start] >= key) {
                swap(start, end);
            }
        }
        //递归
        if (start > low) sort(low, start - 1);
        if (end < high) sort(end + 1, high);
    }


    private int binarySearch(int fromIndex, int toIndex, long key) {
        int low = fromIndex;
        int high = toIndex - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            long midVal = this.keys[mid];
            int cmp = Long.compare(midVal, key);
            if (cmp < 0)
                low = mid + 1;
            else if (cmp > 0)
                high = mid - 1;
            else
                return mid; // keys found
        }
        return -(low + 1);  // keys not found.
    }

    public void swap(int i, int j) {
        if (i == j) return;
        keys[i] ^= keys[j];
        keys[j] ^= keys[i];
        keys[i] ^= keys[j];

        offsetInts[i] ^= offsetInts[j];
        offsetInts[j] ^= offsetInts[i];
        offsetInts[i] ^= offsetInts[j];
    }

}
