package com.splicemachine.si.data.light;


import com.splicemachine.concurrent.Clock;

import java.lang.InterruptedException;
import java.lang.Override;
import java.util.concurrent.TimeUnit;

public class ManualClock implements Clock{
    private long time = 0;

    public void setTime(long time) {
        this.time = time;
    }

    @Override
    public long currentTimeMillis() {
        return time;
    }

    @Override
    public long nanoTime(){
        return TimeUnit.MILLISECONDS.toNanos(time);
    }

    @Override
    public void sleep(long time, TimeUnit unit) throws InterruptedException{
        time+=unit.toMillis(time);
    }
}
