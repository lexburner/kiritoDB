package moe.cnkirito.kiritodb;

import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.nio.Buffer;

public class UnsafeUtil {

    private static final Unsafe UNSAFE;

    private static long ADDRESS_FIELD_OFFSET;

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

    static {
        final Field addressField;
        try {
            addressField = Buffer.class.getDeclaredField("address");
            addressField.setAccessible(true);
            ADDRESS_FIELD_OFFSET = UNSAFE.objectFieldOffset(addressField);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static final long BYTE_ARRAY_BASE_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);

    //这里抄了部分Netty的PlatformDependent0函数
    static void putByte(long address, byte value) {
        UNSAFE.putByte(address, value);
    }

    static byte getByte(long address) {
        return UNSAFE.getByte(address);
    }

    static void putByte(byte[] data, int index, byte value) {
        UNSAFE.putByte(data, BYTE_ARRAY_BASE_OFFSET + index, value);
    }

    static byte getByte(byte[] data, int index) {
        return UNSAFE.getByte(data, BYTE_ARRAY_BASE_OFFSET + index);
    }

    static long getAddress(Object object) {
        return UNSAFE.getLong(object, ADDRESS_FIELD_OFFSET);
    }
}