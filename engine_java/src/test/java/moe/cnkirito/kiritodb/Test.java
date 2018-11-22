package moe.cnkirito.kiritodb;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class Test {
    private static final Lock lock = new ReentrantLock();

    private static final Condition c = lock.newCondition();

    public static void main(String[] args) throws InterruptedException {

        for (int i = 0; i < 10; i++) {
            final int finalI = i;
            new Thread(new Runnable() {
                @Override
                public void run() {
                    lock.lock();
                    try {
                        c.await();
                        System.out.println(finalI);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } finally {
                        lock.unlock();
                    }
                }
            }).start();
        }

        Thread.sleep(2000);

        lock.lock();

        c.signalAll();

        lock.unlock();

        Thread.sleep(100000);
    }
}