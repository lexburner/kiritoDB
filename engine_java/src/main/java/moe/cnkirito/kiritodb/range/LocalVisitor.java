package moe.cnkirito.kiritodb.range;

import com.alibabacloud.polar_race.engine.common.AbstractVisitor;
import moe.cnkirito.kiritodb.common.Util;

public class LocalVisitor extends AbstractVisitor {

    @Override
    public void visit(byte[] key, byte[] value) {
//        if (Thread.currentThread().getName().equals("thread1")) {
//            System.out.println(Util.bytes2Long(key) + ":" + Util.bytes2Long(value));
//        }
    }
}
