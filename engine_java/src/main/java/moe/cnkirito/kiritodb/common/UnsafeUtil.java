package moe.cnkirito.kiritodb.common;

import sun.misc.Unsafe;

import java.lang.reflect.Field;

public class UnsafeUtil {

    public static final Unsafe UNSAFE;

    static {
        try {
            //由于Unsafe是个单列，所以需要通过反射方式获取到
            Field field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            UNSAFE = (Unsafe) field.get(null);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}