//package moe.cnkirito.kiritodb;
//
//import moe.cnkirito.kiritodb.common.Util;
//import moe.cnkirito.kiritodb.partition.FirstBytePartitoner;
//import org.junit.Test;
//
//public class SemaphoreTest {
//
//    @Test
//    public void test1() {
//        System.out.println(Util.long2bytes(Long.MAX_VALUE)[0] & 0xff);
//        System.out.println(Util.long2bytes(Long.MIN_VALUE)[0] & 0xff);
//        System.out.println(Util.long2bytes(1L)[0] & 0xff);
//
//        FirstBytePartitoner firstBytePartitoner = new FirstBytePartitoner();
//
//        System.out.println(Util.long2bytes(122222222222222222L)[0] & 0xff);
//        System.out.println(firstBytePartitoner.getPartition(Util.long2bytes(122222222222222222L)));
////        FirstBytePartitoner firstBytePartitoner = new FirstBytePartitoner();
////        for(byte low = Byte.MIN_VALUE;low < Byte.MAX_VALUE;low++){
////            System.out.println(firstBytePartitoner.getPartition(new byte[]{low}));
////        }
//
//        for(byte i = Byte.MIN_VALUE ;i<Byte.MAX_VALUE-1;i++){
//            System.out.println(i & 0xffL);
//        }
//
//    }
//}
