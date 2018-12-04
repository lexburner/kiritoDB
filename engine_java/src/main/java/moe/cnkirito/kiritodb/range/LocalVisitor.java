package moe.cnkirito.kiritodb.range;

import com.alibabacloud.polar_race.engine.common.AbstractVisitor;
import moe.cnkirito.kiritodb.common.Util;
import moe.cnkirito.kiritodb.partition.HighTenPartitioner;

import java.util.concurrent.atomic.AtomicInteger;

public class LocalVisitor extends AbstractVisitor {

    private byte[] beforeKey = null;
    static AtomicInteger atomicInteger = new AtomicInteger(0);

    @Override
    public void visit(byte[] key, byte[] value) {
        if (beforeKey != null) {
            int result = LocalVisitor.compareByteArrays(beforeKey, key);
            if (result > 0) {
                HighTenPartitioner highTenPartitioner = new HighTenPartitioner();
                System.out.println(atomicInteger.getAndIncrement() + "check range correct error, key is not in order,partition" + highTenPartitioner.getPartition(beforeKey) + " beforeKey=" + Util.bytes2Long(beforeKey) + ",partition " + highTenPartitioner.getPartition(key) + " key=" + Util.bytes2Long(key));
            }
        }
        beforeKey = key;
    }

    public static int compareByteArrays(byte[] source, byte[] other) {
        int length = Math.min(source.length, other.length);
        for (int i = 0; i < length; i++) {
            int sourceByte = source[i] & 0xff;
            int otherType = other[i] & 0xff;
            if (sourceByte != otherType) {
                return sourceByte - otherType;
            }
        }
        return source.length - other.length;
    }

}
