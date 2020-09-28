package com.doubleysoft.delayquene4j;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;

@Data
public class DelayedProperties {
    private static RejectedExecutionHandler rejectedExecutionHandler = (r, executor) -> {
        try {
            TimeUnit.SECONDS.sleep(1);
            executor.submit(r);
        } catch (InterruptedException | RejectedExecutionException e) {
            throw new RuntimeException("[Delay Queue] Fail in add task to thread pool because poll is full");
        }
    };
    /**
     * min pull data period, this param will determine precision,
     * for example, current time is 1000, minPeriod is 5. when a message is arrived now,
     * and it's delay is configured 10006, this item will be handle in time 10005.
     */
    private Long minPeriod = 1l;

    private int corePoolSize = 5;

    private int maximumPoolSize = 10;

    private long keepAliveTime = 60;

    private ExecutorService clientExecutorService = null;

    public ExecutorService getClientExecutorService(){
        if(clientExecutorService != null) {
            return clientExecutorService;
        }
        ThreadFactory FACTORY = new ThreadFactory() {
            private final AtomicInteger integer = new AtomicInteger();
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "Delayed-ThreadPool-" + integer.getAndIncrement());
            }
        };
        clientExecutorService = new ThreadPoolExecutor(this.getCorePoolSize(),
                this.getMaximumPoolSize(), this.getKeepAliveTime(),
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(100), FACTORY);
        ((ThreadPoolExecutor) clientExecutorService).setRejectedExecutionHandler(rejectedExecutionHandler);
        return clientExecutorService;
    }
}
