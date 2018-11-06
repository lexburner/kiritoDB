package com.alibabacloud.polar_race.engine.common;

import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.alibabacloud.polar_race.engine.common.exceptions.RetCodeEnum;
import moe.cnkirito.kiritodb.KiritoDB;

public class EngineRace extends AbstractEngine {

    KiritoDB kiritoDB = new KiritoDB();

    @Override
    public void open(String path) throws EngineException {
        kiritoDB.open(path);
    }

    @Override
    public void write(byte[] key, byte[] value) throws EngineException {
        kiritoDB.write(key, value);
    }

    @Override
    public byte[] read(byte[] key) throws EngineException {
        byte[] value = kiritoDB.read(key);
        if(value == null) throw new EngineException(RetCodeEnum.NOT_FOUND,"key不存在");
        return value;
    }

    @Override
    public void range(byte[] lower, byte[] upper, AbstractVisitor visitor) throws EngineException {
    }

    @Override
    public void close() {
        kiritoDB.close();
    }

}
