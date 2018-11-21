package com.alibabacloud.polar_race.engine.common;

import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import moe.cnkirito.kiritodb.range.LocalVisitor;

import java.util.concurrent.ExecutorService;

public class RangeTest {

    public static void main(String[] args) throws EngineException {
        long start = System.currentTimeMillis();
        final EngineRace engine = new EngineRace();
        engine.open("/tmp/kiritoDB");

        Thread[] threads = new Thread[64];
        for (int i = 0; i < 1; i++) {
            threads[i] = new Thread(() -> {
                try {
                    engine.range(null, null, new LocalVisitor());
                } catch (EngineException e) {
                    e.printStackTrace();
                }
            }, "thread" + i);
        }
        for (int i = 0; i < 1; i++) {
            threads[i].start();
        }
        for (int i = 0; i < 1; i++) {
            try {
                threads[i].join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        engine.close();
        long end = System.currentTimeMillis();
        System.out.println("耗时：" + (end - start) + "ms");
    }

}
