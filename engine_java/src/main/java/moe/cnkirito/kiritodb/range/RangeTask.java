package moe.cnkirito.kiritodb.range;

import com.alibabacloud.polar_race.engine.common.AbstractVisitor;

import java.util.concurrent.CountDownLatch;

public class RangeTask {
    private AbstractVisitor abstractVisitor;
    private CountDownLatch countDownLatch;

    public RangeTask(AbstractVisitor abstractVisitor, CountDownLatch countDownLatch) {
        this.abstractVisitor = abstractVisitor;
        this.countDownLatch = countDownLatch;
    }

    public AbstractVisitor getAbstractVisitor() {
        return abstractVisitor;
    }

    public void setAbstractVisitor(AbstractVisitor abstractVisitor) {
        this.abstractVisitor = abstractVisitor;
    }

    public CountDownLatch getCountDownLatch() {
        return countDownLatch;
    }

    public void setCountDownLatch(CountDownLatch countDownLatch) {
        this.countDownLatch = countDownLatch;
    }
}
