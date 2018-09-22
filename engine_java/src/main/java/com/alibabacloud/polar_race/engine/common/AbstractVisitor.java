package com.alibabacloud.polar_race.engine.common;

public abstract class AbstractVisitor {
    public abstract void visit(byte[] key, byte[] value);
}
