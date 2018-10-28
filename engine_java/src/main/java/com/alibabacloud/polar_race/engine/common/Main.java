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
        String content = "";
        for(int i=0;i<4 * 1024;i++){
            content += "1";
        }
        engineRace.write("hello113".getBytes(), content.getBytes());
        engineRace.write("hello114".getBytes(), content.getBytes());
        byte[] read = engineRace.read("hello112".getBytes());
        if(read!=null){
            System.out.println(new String(read));
        }else System.out.println("null");

    }
}
