package moe.cnkirito.kiritodb.partition;

public interface Partitionable {
    int getPartition(byte[] key);
}
