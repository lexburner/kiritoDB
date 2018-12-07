package moe.cnkirito.kiritodb.range;

import java.nio.ByteBuffer;

/**
 * @author daofeng.xjf
 * @date 2018/12/7
 */
public class CacheItem {

    public volatile int dbIndex;
    public volatile int useRef;
    public volatile boolean ready;
    public volatile boolean allReach;
    public volatile ByteBuffer buffer;

}
