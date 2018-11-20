package moe.cnkirito.kiritodb;

import moe.cnkirito.kiritodb.common.Util;
import moe.cnkirito.kiritodb.partition.FirstBytePartitoner;
import org.junit.Test;

public class SemaphoreTest {

    @Test
    public void test1() {
        System.out.println(Util.long2bytes(Long.MAX_VALUE)[0] & 0xff);
        System.out.println(Util.long2bytes(Long.MIN_VALUE)[0] & 0xff);
        System.out.println(Util.long2bytes(1L)[0] & 0xff);
        long test = 122222222222222222L;
//        byte[] bytes = Util.long2bytes(test);
//        System.out.println(((bytes[0] & 0xff) << 3) | (bytes[1] & 0xff));
//        System.out.println(test >>> (64 - 11));

//        int len = 64000000;
//        long base = Long.MAX_VALUE / len;
//        FirstBytePartitoner firstBytePartitoner = new FirstBytePartitoner();
//        for (int i = 0; i < len; i++) {
//            System.out.println(firstBytePartitoner.getPartition(Util.long2bytes(i * base)));
//        }
    }
}
