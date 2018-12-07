### 1. 赛题背景

本次赛题是以 PolarDB 为背景，以 Optane SSD 为存储介质，参赛者在其基础之上探索实现一种高效的 kv 存储引擎。

### 2. 赛题描述

实现一个简化、高效的 kv 存储引擎，初赛要求实现 Write，Read 接口

```java
public abstract void write(byte[] key, byte[] value) throws EngineException;
public abstract byte[] read(byte[] key) throws EngineException;
```

复赛要求实现一个 Range 接口

```java
public abstract void range(byte[] lower, byte[] upper, AbstractVisitor visitor) throws EngineException;
```

Write 和 Read 很好理解，Write 就是存放一个键值对，Read 则可以根据键返回对应值，Range 要求实现的是一个批量查询的接口，按照字节序遍历一定范围内键值对，并通过 visitor 回调，让评测程序去验证。

程序评测逻辑分为2个阶段：
1）Recover正确性评测
此阶段评测程序会并发写入特定数据（key 8B、value 4KB）同时进行任意次kill -9来模拟进程意外退出（参赛引擎需要保证进程意外退出时数据持久化不丢失），接着重新打开DB，调用Read接口来进行正确性校验

2）性能评测
-  随机写入：64个线程并发随机写入，每个线程使用Write各写100万次随机数据（key 8B、value 4KB）
-  随机读取：64个线程并发随机读取，每个线程各使用Read读取100万次随机数据
注：2.2阶段会对所有读取的kv校验是否匹配，如不通过则终止，评测失败


注：
1. 共3个维度测试性能，每一个维度测试结束后会保留DB数据，关闭Engine实例，重启进程，清空PageCache，下一个维度开始后重新打开新的Engine实例
2. 读取阶段会随机进行样本数据抽样校验，没有通过的话则评测不通过
3. 参赛引擎只需保证进程意外退出时数据持久化不丢失即可，不要求保证在系统crash时的数据持久化不丢失

### 3. 赛题分析
key 的长度固定为 8 字节，因此可使用 uint64_t 表示。

value 的长度固定 4096 字节，并且不可压缩，因此比不保存 value 的长度，可使用 LBA(Logical Block Addressing) 表示数据的位置。

读写操作不会在同一次测试中发生，因此不需要维护“动态索引”。

不要求“掉电不丢数据”，只要求“进程退出不丢数据”，因此可充分利用 page cache，索引不写透数据，data 写不加 O_SYNC。

数据分 partition，减少冲突，加大并行度。

性能评测中会有2次带数据重启，DB 加载的开销也计入总体时间，因此加载过程需要并行化。

读写尽量使用大块 I/O。

range 操作是本次比赛最关键的部分，因此能否设计出最有利于 range 的架构则是争夺第一的关键。

### 优化点

#### jvm 调优

打印 GC 日志
-XX:+PrintGCDetails -XX:+PrintGCDateStamps

newRatio=1 -> newRatio=4
众所周知 newRatio 是控制 young 区和 old 区大小比例的，newRatio=1 -> newRatio=4 提升老年代的大小直接让成绩优化了 6s

原因分析：
（1）young 区过大，对象在年轻代待得太久，多次拷贝；
    
（2）old 区过小，cms gc次数过多
    newRatio=1 123次 cms gc
    newRatio=4 5次 cms gc

CMS GC 控制
建立在上面分析的基础之上的一个优化思路：UseCMSInitiatingOccupancyOnly 和 CMSInitiatingOccupancyFraction，两个参数用起来，可以自己确定老年代使用空间达到多少触发 CMS GC


#### 堆内内存和堆外内存的坑点

分配方式：
堆内内存 ByteBuffer.allocate(capacity)
堆外内存 ByteBuffer.allocateDirect(capacity)

底层实现:
堆内内存 数组，JVM 内存
堆外内存 unsafe.allocateMemory(size)，返回直接内存的首地址

分配大小限制：
堆内内存 和 -Xms-Xmx 配置的 JVM 内存相关，并且数组的大小有限制，在做测试时发现，当 JVM free memory 大于 1.5G 时，ByteBuffer.allocate(900M) 时会报错
堆外内存 可以通过 -XX:MaxDirectMemorySize 参数从 JVM 层面去限制，同时受到机器虚拟内存（说物理内存不太准确）的限制

垃圾回收：
两者都可以自动回收
堆内内存 自然不必多说
堆外内存 当 DirectByteBuffer 不再被使用时，会出发内部 cleaner 的钩子，比赛中为了保险起见，可以考虑手动回收
```java
((DirectBuffer) buffer).cleaner().clean();
```

-server -XX:-UseBiasedLocking -Xms2100m -Xmx2100m -XX:NewSize=1500m -XX:MaxMetaspaceSize=32m -XX:MaxDirectMemorySize=1G -XX:+UseG1GC

#### range缓存算法

1024 个分区 每个分区 256M
可用内存 1024M
缓存粒度（预读粒度）：一个分区
共计 4 个缓存块

cacheItem 4

class CacheItem{
    int dbIndex;
    int readRef;
    boolean canRead;
    ByteBuffer buffer;
}

64 个 visit 线程


4 个 fetch 线程

for(int i=0;i<partitionNum;i++){
    while(true){
        if(cacheItem[window].readRef == 0) {
          break;
        }
    }
    load(cacheItem[window].buffer);
}



