package moe.cnkirito.kiritodb.common;

/**
 * @author daofeng.xjf
 * @date 2018/11/30
 */
public class LoopQuerySemaphore {

    private volatile boolean permits;

    public LoopQuerySemaphore(int permits) {
        if(permits>0){
            this.permits = true;
        }else {
            this.permits = false;
        }
    }

    public void acquire() throws InterruptedException {
        while (!permits){
            Thread.sleep(1);
        }
        permits = false;
    }

    public void release(){
        permits = true;
    }

}
