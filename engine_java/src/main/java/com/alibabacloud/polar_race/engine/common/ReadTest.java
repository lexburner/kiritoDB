package com.alibabacloud.polar_race.engine.common;

import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import moe.cnkirito.kiritodb.common.Util;

/**
 * 读测试
 */
public class ReadTest {

    public static void main(String[] args) throws EngineException {
        // 记录启动时间
        long start = System.currentTimeMillis();

        EngineRace engine = new EngineRace();
        engine.open("/tmp/kiritoDB");
        int len = 640000;
        byte[] hashByte = new byte[Byte.MAX_VALUE-Byte.MIN_VALUE+1];
        byte now = Byte.MIN_VALUE;
        for(int i = 0;i<  (Byte.MAX_VALUE-Byte.MIN_VALUE+1);i++){
            hashByte[i] = now++;
        }
        for (int i = 0; i < len; i++) {
            try {
                byte[] bytes = Util.long2bytes(i);
                bytes[0] = hashByte[i % (Byte.MAX_VALUE-Byte.MIN_VALUE+1)];
                byte[] bs = engine.read(bytes);
                long ans = Util.bytes2Long(bs);
                if (i != ans) {
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
