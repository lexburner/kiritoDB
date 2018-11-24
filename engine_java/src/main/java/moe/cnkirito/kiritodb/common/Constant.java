package moe.cnkirito.kiritodb.common;

public class Constant {

    public static final String DATA_PREFIX = "/data";
    public static final String DATA_SUFFIX = ".polar";
    public static final String INDEX_PREFIX = "/index";
    public static final String INDEX_SUFFIX = ".polar";
    public static final int VALUE_LENGTH = 4 * 1024;
    public static final int INDEX_LENGTH = 8;

    public static final int expectedNumPerPartition = 64000 * 2;
    public static int partitionNum = 1 << 9;
}
