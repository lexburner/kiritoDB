package moe.cnkirito.kiritodb;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ConditionTest {

    private Lock partitionLock = new ReentrantLock();
    private Condition writeCondition = partitionLock.newCondition();
    private Condition readCondition = partitionLock.newCondition();


    public static void main(String[] args) {

        ConditionTest conditionTest = new ConditionTest();
        conditionTest.test();

    }

    public void test(){
        new Thread(()->{
            partitionLock.lock();
            try{
                writeCondition.await();
                System.out.println("1");
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                partitionLock.unlock();
            }
        }).start();

        new Thread(()->{
            partitionLock.lock();
            try{
                readCondition.await();
                System.out.println("2");
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                partitionLock.unlock();
            }
        }).start();

        partitionLock.lock();
        try{
            System.out.println("==1");
            Thread.sleep(2000);
            writeCondition.signalAll();
            readCondition.signalAll();
            System.out.println("==2");
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            partitionLock.unlock();
        }

    }

}
