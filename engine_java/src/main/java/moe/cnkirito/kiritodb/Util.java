package moe.cnkirito.kiritodb;

import sun.misc.Unsafe;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Util {
    /**
     * 当前时间
     *
     * @return
     */
    public static String curTime() {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//设置日期格式
        // new Date()为获取当前系统时间
        return df.format(new Date());
    }

    /**
     * 当前进程
     *
     * @return
     */
    public static String pid() {
        String name = ManagementFactory.getRuntimeMXBean().getName();
        String pid = name.split("@")[0];
        return pid;
    }

    /**
     * 执行shell指令
     *
     * @param cmd
     * @return
     * @throws IOException
     */
    public static String runCmd(String cmd) throws IOException {
        Process process = Runtime.getRuntime().exec(cmd);
        InputStream is = process.getInputStream();
        BufferedReader reader = new BufferedReader(new InputStreamReader(is));
        StringBuffer sb = new StringBuffer();
        String tmp;
        int index = 0;
        while ((tmp = reader.readLine()) != null && index < 20) {
            sb.append(tmp).append("\n");
            ++index;
        }
        process.destroy();
        return sb.toString();
    }

    /**
     * bytes转long
     *
     * @param buffer
     * @return
     */
    public static Long bytes2Long(byte[] buffer) {
        long values = 0;
        int len = 8;
        // 8 与 buffer.length 较小者
        len = len > buffer.length ? buffer.length : len;
        for (int i = 0; i < len; ++i) {
            values <<= 8;
            values |= (buffer[i] & 0xff);
        }
        return values;
    }

    /**
     * long转bytes
     *
     * @param values
     * @return
     */
    public static byte[] long2bytes(long values) {
        byte[] buffer = new byte[8];
        for (int i = 0; i < 8; ++i) {
            int offset = 64 - (i + 1) * 8;
            buffer[i] = (byte) ((values >> offset) & 0xff);
        }
        return buffer;
    }

    /**
     * long转bytes
     *
     * @param values
     * @return
     */
    public static byte[] int2bytes(int values) {
        byte[] buffer = new byte[4];
        for (int i = 0; i < 4; ++i) {
            int offset = 32 - (i + 1) * 8;
            buffer[i] = (byte) ((values >> offset) & 0xff);
        }
        return buffer;
    }

    /**
     * 模拟随机生成的4kb字节
     *
     * @param l
     * @return
     */
    public static byte[] _4kb(long l) {
        ByteBuffer buffer = ByteBuffer.allocate(4 * 1024);
        buffer.putLong(l);
        for (int i = 0; i < 4048 - 8; ++i) {
            buffer.put((byte) 0);
        }
        return buffer.array();
    }

    public static Unsafe getUnsafe() {
        try {
            Field singletonInstanceField = Unsafe.class.getDeclaredField("theUnsafe");
            singletonInstanceField.setAccessible(true);
            return (Unsafe) singletonInstanceField.get(null);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            e.printStackTrace();
        }
        return null;
    }

}
