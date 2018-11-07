package moe.cnkirito.kiritodb;

import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;

/**
 * 字节池
 */
public class BytesPoolableFactory extends BasePooledObjectFactory<byte[]> {

    private int size;

    public BytesPoolableFactory(int len) {
        this.size = len;
    }

    public byte[] create() {
        return new byte[size];
    }

    public PooledObject<byte[]> wrap(byte[] bytes) {
        return new DefaultPooledObject<>(bytes);
    }
}
