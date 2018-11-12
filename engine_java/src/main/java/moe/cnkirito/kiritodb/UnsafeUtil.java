package moe.cnkirito.kiritodb;

import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.nio.Buffer;

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

    private static final long BYTE_ARRAY_BASE_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);

}