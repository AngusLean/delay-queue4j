package com.doubleysoft.delayquene4j.tasks;

import com.doubleysoft.delayquene4j.model.DelayedInfoDTO;
import com.doubleysoft.delayquene4j.support.JsonProvider;
import com.doubleysoft.delayquene4j.support.NamedThreadFactory;
import com.doubleysoft.delayquene4j.support.RedisProvider;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;

/**
 * @author dongyang.yu
 * @email dongyang.yu@anxincloud.com
 */
@Slf4j
public class PullInTimeMsgTask implements Runnable, PullMixin, ShutDownCallBack {
    private volatile boolean isStop = false;
    private final RedisProvider redisProvider;
    private final ExecutorService busiExecutorService;
    private final ExecutorService bgExecutorService;
    private final JsonProvider jsonProvider;
    private int errorCount = 0;

    public PullInTimeMsgTask(RedisProvider redisProvider, JsonProvider jsonProvider, ExecutorService busiExecutorService) {
        this(redisProvider, jsonProvider, busiExecutorService, Executors.newCachedThreadPool(
                new NamedThreadFactory("DELAY_BLOCK_CONSUME_", true)));
    }

    public PullInTimeMsgTask(RedisProvider redisProvider, JsonProvider jsonProvider, ExecutorService busiExecutorService, ExecutorService pullOutTimeService) {
        this.redisProvider = redisProvider;
        this.busiExecutorService = busiExecutorService;
        this.jsonProvider = jsonProvider;
        this.bgExecutorService = pullOutTimeService;
        run();
    }

    public void run() {
        HandlerContext.setHandlerKeyChangeCallBack(systemKey -> {
            //each pull handler just pull interest keys
            bgExecutorService.execute(() -> {
                doSystemCallBack(systemKey);
            });
        });
    }

    private void doSystemCallBack(String systemKey) {
        while (!isStop && errorCount < Constants.MAX_ERROR_COUNT) {
            try {
                doFetchMsg(getWaitHandleSetName(systemKey));
                errorCount--;
            } catch (Exception e) {
                log.warn("[Delay Queue]Fail in handle key:{} to DelayedInfoDTO class", systemKey);
                errorCount++;
                simpleSleep();
            }
        }
        log.info("[Delay Queue] Pull intime message thread shutdown");
        if (!bgExecutorService.isShutdown()) {
            bgExecutorService.shutdown();
        }
    }

    /**
     * fetch all message that need to be handle immediately
     */
    private void doFetchMsg(String blockingKeyName) {
        //block fetch
        String msg = redisProvider.blockPopFromList(blockingKeyName);
        if (msg == null || msg.length() == 0) {
            return;
        }
        DelayedInfoDTO delayedInfoDTO;
        try {
            delayedInfoDTO = jsonProvider.fromJSONString(msg, DelayedInfoDTO.class);
        } catch (Exception e) {
            log.warn("[Delay Queue]Fail in parse string:{} to DelayedInfoDTO class", msg);
            return;
        }
        busiExecutorService.execute(() -> {
            HandlerContext.getMsgHandler(getScoredSetName(delayedInfoDTO.getSystem()))
                    .handle(delayedInfoDTO.getUuid(), delayedInfoDTO.getMessage());
        });
    }

    @Override
    public void stop() {
        isStop = true;
    }

    private void simpleSleep() {
        try {
            TimeUnit.SECONDS.sleep((errorCount / 2));
        } catch (InterruptedException ignore) {
        }
    }

}
