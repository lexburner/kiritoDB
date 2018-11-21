package moe.cnkirito.kiritodb;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ConditionHolder {

    private Lock partitionLock = new ReentrantLock();
    private Condition writeCondition = partitionLock.newCondition();

    public void range(){
        partitionLock.lock();
        try{
            writeCondition.await();
            System.out.println("1");
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            partitionLock.unlock();
        }
    }

}
