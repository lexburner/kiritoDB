package com.alibabacloud.polar_race.engine.common;

import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import moe.cnkirito.kiritodb.range.LocalVisitor;

public class RangeTest {

    public static void main(String[] args) throws EngineException {
        new RangeTest().test();
    }

    public void test() throws EngineException {
        long start = System.currentTimeMillis();
        final EngineRace engine = new EngineRace();
        engine.open("/tmp/kiritoDB");

        for (int k = 0; k < 2; k++) {
            Thread[] threads = new Thread[64];
            for (int i = 0; i < 64; i++) {
                threads[i] = new Thread(() -> {
                    try {
                        engine.range(null, null, new LocalVisitor());
                    } catch (EngineException e) {
                        e.printStackTrace();
                    }
                }, "thread" + i);
            }
            for (int i = 0; i < 64; i++) {
                threads[i].start();
            }
            for (int i = 0; i < 64; i++) {
                try {
                    threads[i].join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            System.out.println("==== loop " + k + " end ===");
        }

        engine.close();
        long end = System.currentTimeMillis();
        System.out.println("range cost " + (end - start) + "ms");
    }

}
