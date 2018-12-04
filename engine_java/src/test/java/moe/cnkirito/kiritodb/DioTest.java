package moe.cnkirito.kiritodb;

import moe.cnkirito.kiritodb.common.Util;
import moe.cnkirito.kiritodb.partition.HighTenPartitioner;
import moe.cnkirito.kiritodb.partition.Partitionable;

public class DioTest {


//    @Test
//    public void testWrite() throws Exception{
//        File file = new File("/tmp/dioTest.data");
//        if(file.exists()){
//            file.delete();
//        }
//        file.createNewFile();
//        int bufferSize = 20 * 1024 * 1024;
//        DirectRandomAccessFile directFile = new DirectRandomAccessFile(file, "rw", bufferSize);
//        for(int i= 0;i< bufferSize / 4096;i++){
//            directFile.write(Util._4kb(i));
//        }
//        directFile.close();
//    }
//
//    @Test
//    public void testRead() throws Exception{
//        File file = new File("/tmp/dioTest.data");
//        int bufferSize = 20 * 1024 * 1024;
//        DirectRandomAccessFile directFile = new DirectRandomAccessFile(file, "rw", bufferSize);
//        for(int i= 0;i< bufferSize / 4096;i++){
//            byte[] buffer = new byte[4 * 1024];
//            directFile.read(buffer);
//            directFile.readFully(buffer);
//            System.out.println();
//        }
//        directFile.close();
//    }

    public static void main(String[] args) {
        Partitionable highTenPartitioner = new HighTenPartitioner();
        System.out.println(highTenPartitioner.getPartition(Util.long2bytes(3139393939392020L)));
        System.out.println(highTenPartitioner.getPartition(Util.long2bytes(3130302020202020L)));
    }

}
