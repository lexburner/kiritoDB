package moe.cnkirito.kiritodb.range;

import java.nio.ByteBuffer;

/**
 * the cache of one partition
 */
public class CacheItem {

    public volatile int dbIndex;
    public volatile int useRef;
    public volatile boolean ready;
    public volatile boolean allReach;
    public volatile ByteBuffer buffer;

}
