package com.alibabacloud.polar_race.engine.common;

import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import moe.cnkirito.kiritodb.Util;

/**
 * 读测试
 */
public class ReadTest {

    public static void main(String[] args) throws EngineException {


        // 记录启动时间
        long start = System.currentTimeMillis();

        EngineRace engine = new EngineRace();
        engine.open("D:/tmp/kiritoDB");
        int len = 640000;
        long base = Long.MAX_VALUE / len;
        for (int i = 0; i < len; i += 256) {
            try {
                byte[] bs = engine.read(Util.long2bytes(i*base));
                long ans = Util.bytes2Long(bs);
                if (i*base != ans) {
                    System.err.println("no equal:" + i);
                }
            } catch (Exception e) {
                System.out.println(i);
                e.printStackTrace();
            }
        }

        engine.close();

        long end = System.currentTimeMillis();
        System.out.println("耗时：" + (end - start) + "ms");
    }

}
