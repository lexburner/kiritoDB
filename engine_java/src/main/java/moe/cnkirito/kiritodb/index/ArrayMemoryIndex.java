package moe.cnkirito.kiritodb.index;

/**
 * the implementation of memory index using java origin array
 */
public class ArrayMemoryIndex implements MemoryIndex {

    private long keys[];
    private int offset[];
    private int indexSize;

    public ArrayMemoryIndex(int initSize) {
        this.keys = new long[initSize];
        this.offset = new int[initSize];
        this.indexSize = initSize;
    }

    @Override
    public int getSize() {
        return this.indexSize;
    }

    @Override
    public void init() {
        this.sortAndCompact();
    }

    @Override
    public void insertIndexCache(long key, int value) {
        this.keys[value] = key;
        this.offset[value] = value;
    }

    @Override
    public int get(long key) {
        return this.binarySearchPosition(key);
    }

    @Override
    public long[] getKeys() {
        return this.keys;
    }

    @Override
    public int[] getOffset() {
        return this.offset;
    }

    /**
     * sort the index and compact the same key
     */
    private void sortAndCompact() {
        if (this.indexSize != 0) {
            sort(0, this.indexSize - 1);
            if (this.indexSize > 60000 && this.indexSize < 64000) {
                return;
            }
            compact();
        }
    }

    private void compact() {
        long[] newKeys = new long[indexSize];
        int[] newOffsetInts = new int[indexSize];

        int curIndex = 0;
        newOffsetInts[0] = this.offset[0];
        newKeys[0] = this.keys[0];
        for (int i = 1; i < this.indexSize; i++) {
            if (this.keys[i] != this.keys[i - 1]) {
                curIndex++;
                newKeys[curIndex] = this.keys[i];
                newOffsetInts[curIndex] = this.offset[i];
            } else {
                newOffsetInts[curIndex] = Math.max(newOffsetInts[curIndex], this.offset[i]);
            }
        }
        this.indexSize = curIndex + 1;
        this.offset = newOffsetInts;
        this.keys = newKeys;
    }

    private int binarySearchPosition(long key) {
        int index = this.binarySearch(0, indexSize, key);
        if (index >= 0) {
            return this.offset[index];
        } else {
            return -1;
        }
    }

    private void sort(int low, int high) {
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

    private void swap(int i, int j) {
        if (i == j) return;
        keys[i] ^= keys[j];
        keys[j] ^= keys[i];
        keys[i] ^= keys[j];

        offset[i] ^= offset[j];
        offset[j] ^= offset[i];
        offset[i] ^= offset[j];
    }

}
