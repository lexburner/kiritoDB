//package moe.cnkirito.kiritodb;
//
//import moe.cnkirito.kiritodb.index.IndexEntry;
//import org.junit.Test;
//
//import java.util.Arrays;
//import java.util.Comparator;
//
//public class BinarySearchTest {
//
//    @Test
//    public void test1() {
//        IndexEntry[] indexEntries = new IndexEntry[50];
//        for (int i = 0; i < 50; i++) {
//            IndexEntry indexEntry = new IndexEntry(i, i / 3);
//            indexEntries[i] = indexEntry;
//        }
//        int index = Arrays.binarySearch(indexEntries, 0, 50, new IndexEntry(250, -1), new Comparator<IndexEntry>() {
//            @Override
//            public int compare(IndexEntry a, IndexEntry b) {
//                return Long.compare(a.getKey(), b.getKey());
//            }
//        });
//        System.out.println(index);
//    }
//
//}
