package com.alibabacloud.polar_race.engine.common;

import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;

/**
 * @author kirito.moe@foxmail.com
 * Date 2018-10-28
 */
public class Main {
    public static void main(String[] args) throws EngineException {
        EngineRace engineRace = new EngineRace();
        engineRace.open("/tmp/kiritoDB");
        String content1 = "";
        for(int i=0;i<4 * 1024;i++){
            content1 += "1";
        }
        String content2 = "";
        for(int i=0;i<4 * 1024;i++){
            content2 += "2";
        }
        String content3 = "";
        for(int i=0;i<4 * 1024;i++){
            content3 += "3";
        }
        String content4 = "";
        for(int i=0;i<4 * 1024;i++){
            content4 += "4";
        }
//        engineRace.write("hello113".getBytes(), content3.getBytes());
//        engineRace.write("hello114".getBytes(), content4.getBytes());
        byte[] read1 = engineRace.read("hello111".getBytes());
        byte[] read2 = engineRace.read("hello112".getBytes());
        byte[] read3 = engineRace.read("hello113".getBytes());
        byte[] read4 = engineRace.read("hello114".getBytes());
        System.out.println(new String(read1));
        System.out.println(new String(read2));
        System.out.println(new String(read3));
        System.out.println(new String(read4));

    }
}
