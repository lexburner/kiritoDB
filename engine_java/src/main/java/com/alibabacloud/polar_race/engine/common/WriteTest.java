package com.alibabacloud.polar_race.engine.common;

import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import moe.cnkirito.kiritodb.common.Util;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 写测试
 */
public class WriteTest {
    public static void main(String[] args) throws EngineException {

        // 记录启动时间
        long start = System.currentTimeMillis();

        // fixed 线程池
        Executor executor = Executors.newFixedThreadPool(64);

        // 打开引擎
        final EngineRace engine = new EngineRace();
        engine.open("/tmp/kiritoDB");
        write(executor, engine, 0);
        ((ExecutorService) executor).shutdownNow();

        System.out.println(getFreeMemory());
        engine.close();
        long end = System.currentTimeMillis();
        System.out.println("耗时：" + (end - start) + "ms");
    }

    private static void write(Executor executor, EngineRace engine, int offset) {
        // 写数据
        final AtomicInteger atomicInteger = new AtomicInteger();
        int len = 640000;
        final CountDownLatch downLatch = new CountDownLatch(len);
        byte[] hashByte = new byte[Byte.MAX_VALUE-Byte.MIN_VALUE+1];
        byte now = Byte.MIN_VALUE;
        for(int i = 0;i<  (Byte.MAX_VALUE-Byte.MIN_VALUE+1);i++){
            hashByte[i] = now++;
        }
        for (int i=0;i<640000;i++) {
            final int cur = i;
            executor.execute(new Runnable() {
                public void run() {
                    try {
                        byte[] bytes = Util.long2bytes(cur);
                        bytes[0] = hashByte[cur % (Byte.MAX_VALUE-Byte.MIN_VALUE+1)];
                        engine.write(bytes, Util._4kb(cur - offset));
                        System.out.println(atomicInteger.incrementAndGet());
                        downLatch.countDown();
                    } catch (EngineException e) {
                        e.printStackTrace();
                    }
                }
            });
        }
        try {
            downLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static String getFreeMemory() {
        long free = Runtime.getRuntime().freeMemory() / 1024 / 1024;
        long total = Runtime.getRuntime().totalMemory() / 1024 / 1024;
        long max = Runtime.getRuntime().maxMemory() / 1024 / 1024;
        return "free=" + free + "M,total=" + total + "M,max=" + max;
    }
}
