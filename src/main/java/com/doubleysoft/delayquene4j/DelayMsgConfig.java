package com.doubleysoft.delayquene4j;

import com.doubleysoft.delayquene4j.model.DelayedInfoDTO;
import com.doubleysoft.delayquene4j.support.JsonProvider;
import com.doubleysoft.delayquene4j.support.LockProvider;
import com.doubleysoft.delayquene4j.support.RedisProvider;
import com.doubleysoft.delayquene4j.support.jackson.JacksonProvider;
import com.doubleysoft.delayquene4j.support.reddison.RedissonRedisLockProvider;
import com.doubleysoft.delayquene4j.support.reddison.RedissonRedisProvider;
import com.doubleysoft.delayquene4j.tasks.PullInTimeMsgTask;
import com.doubleysoft.delayquene4j.tasks.PullOutTimeMsgTask;
import com.doubleysoft.delayquene4j.tasks.RedisDelayMsgService;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RedissonClient;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author dongyang.yu
 * @email dongyang.yu@anxincloud.com
 */
@Slf4j
public class DelayMsgConfig implements DelayMsgService {
    /**
     * min pull data period, this param will determine precision,
     * for example, current time is 1000, minPeriod is 5. when a message is arrived now,
     * and it's delay is configured 10006, this item will be handle in time 10005.
     */
    @Getter
    @Setter
    private Long minPeriod = 1l;

    @Setter
    private int corePoolSize = 5;

    @Setter
    private int maximumPoolSize = 10;

    @Setter
    private long keepAliveTime = 60;

    private LockProvider lockProvider;
    private RedisProvider redisProvider;
    private JsonProvider jsonProvider;
    private DelayMsgService delayMsgService;
    private ExecutorService executorService;

    private RejectedExecutionHandler rejectedExecutionHandler = (r, executor) -> {
        try {
            TimeUnit.SECONDS.sleep(1);
            try {
                executor.submit(r);
            } catch (RejectedExecutionException e) {
                log.error("[Delay Queue] Fail in add task to thread pool because poll is full");
            }
        } catch (InterruptedException ignore) {
        }
    };

    public DelayMsgConfig(RedissonClient redissonClient) {
        this(new RedissonRedisProvider(redissonClient), new RedissonRedisLockProvider(redissonClient), new JacksonProvider());
    }

    public DelayMsgConfig(RedisProvider redisProvider, LockProvider lockProvider, JsonProvider jsonProvider) {
        this.redisProvider = redisProvider;
        this.lockProvider = lockProvider;
        this.jsonProvider = jsonProvider;
        this.delayMsgService = new RedisDelayMsgService(redisProvider, jsonProvider);
    }

    public void begin() {
        initThreadPool();
        beginTimerTasks();
    }

    @Override
    public void addDelayMessage(DelayedInfoDTO delayedInfoDTO, DelayedMsgHandler msgHandler) {
        this.delayMsgService.addDelayMessage(delayedInfoDTO, msgHandler);
    }

    @Override
    public void addDelayMessage(DelayedInfoDTO delayedInfoDTO) {
        this.delayMsgService.addDelayMessage(delayedInfoDTO);
    }

    @Override
    public void addDelayCallBack(String system, DelayedMsgHandler msgHandler) {
        this.delayMsgService.addDelayCallBack(system, msgHandler);
    }

    private void beginTimerTasks() {
        PullOutTimeMsgTask pullOutTimeMsgTask = new PullOutTimeMsgTask(lockProvider, redisProvider, executorService, this);
        PullInTimeMsgTask pullInTimeMsgTask = new PullInTimeMsgTask(redisProvider, jsonProvider, executorService);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            pullInTimeMsgTask.stop();
            pullOutTimeMsgTask.stop();
        }));
    }

    private void initThreadPool() {
        ThreadFactory FACTORY = new ThreadFactory() {
            private final AtomicInteger integer = new AtomicInteger();

            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "Delayed-ThreadPool-" + integer.getAndIncrement());
            }
        };
        executorService = new ThreadPoolExecutor(corePoolSize, maximumPoolSize, keepAliveTime, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(100), FACTORY);
        ((ThreadPoolExecutor) executorService).setRejectedExecutionHandler(rejectedExecutionHandler);
    }


}
