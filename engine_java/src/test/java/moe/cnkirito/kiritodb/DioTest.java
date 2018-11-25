package moe.cnkirito.kiritodb;

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
//            directFile.mmapWrite(Util._4kb(i));
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

}
