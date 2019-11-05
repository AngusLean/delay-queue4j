package com.doubleysoft.delayquene4j.tasks;

import com.doubleysoft.delayquene4j.DelayMsgConfig;
import com.doubleysoft.delayquene4j.support.LockProvider;
import com.doubleysoft.delayquene4j.support.RedisProvider;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


@Slf4j
public class PullOutTimeMsgTask implements Runnable, PullMixin, ShutDownCallBack {
    private volatile boolean isStop = false;

    private final ExecutorService executorService;
    private final LockProvider lockProvider;
    private final RedisProvider redisProvider;
    private final DelayMsgConfig delayMsgConfig;

    private ScheduledExecutorService timedPullService;

    public PullOutTimeMsgTask(LockProvider lockProvider,
                              RedisProvider redisProvider, ExecutorService executorService, DelayMsgConfig delayMsgConfig) {

        this.executorService = executorService;
        this.lockProvider = lockProvider;
        this.redisProvider = redisProvider;
        this.delayMsgConfig = delayMsgConfig;
        timedPullService = Executors.newSingleThreadScheduledExecutor();
        timedPullService.scheduleAtFixedRate(this, delayMsgConfig.getMinPeriod(), delayMsgConfig.getMinPeriod(), TimeUnit.SECONDS);
    }

    public void doPullAllTopics() {
        Set<String> allTopics = redisProvider.getFromSet(Constants.ALL_TOPIC_SET_NAME);
        if (allTopics == null || allTopics.isEmpty()) {
            return;
        }
        allTopics.forEach(row -> {
            executorService.submit(() -> {
                try {
                    doPullTimeOutMsg(row);
                } catch (Exception e) {
                    log.warn("[Delay Queue] Fail in pull message from queue :{}", row, e);
                }
            });
        });
    }

    private void doPullTimeOutMsg(String queueName) {
        long crt = System.currentTimeMillis() / 1000;
        //query begin score should ensure no time-slice between last pull action and this pull action.
        Long range = crt + delayMsgConfig.getMinPeriod();
        crt = 0;
        //delayed message need to be processed now, but we add it to redis queue for performance
        //in distributed system
        String lockName = getLockKey(queueName);
        try {
            List<String> fromZSetByScore = redisProvider.getFromZSetByScore(queueName, crt, range);
            if (fromZSetByScore == null || fromZSetByScore.isEmpty()) {
                return;
            }
            boolean lockResult = lockProvider.tryLock(lockName, 0l);
            if (!lockResult) {
                return;
            }
            log.info("[Delay Queue] find Delayed message:{}, {}", fromZSetByScore, System.currentTimeMillis());
            String blockingKey = getWaitHandleSetName(queueName);
            redisProvider.removeFromZSetAndAdd2BlockQueue(queueName, crt, range, blockingKey, fromZSetByScore);
        } catch (Exception e) {
            log.error("[Delay Queue] Fail in tryLock queueName:{}", queueName);
        } finally {
            lockProvider.release(lockName);
        }
    }


    @Override
    public void run() {
        try {
            if (!isStop) {
                doPullAllTopics();
            } else {
                log.info("[Delay Queue] Pull OutTime message shutdown");
                try {
                    this.timedPullService.shutdown();
                } catch (Exception ignore) {
                }
            }

        } catch (Exception ignore) {
        }
    }

    @Override
    public void stop() {
        isStop = true;
    }
}
