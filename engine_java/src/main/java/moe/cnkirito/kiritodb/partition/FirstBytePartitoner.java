package moe.cnkirito.kiritodb.partition;

public class FirstBytePartitoner implements Partitionable {
    @Override
    public int getPartition(byte[] key) {
        return key[0] & 0xff;
    }
}
