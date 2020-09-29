package com.doubleysoft.delayquene4j;

import com.doubleysoft.delayquene4j.model.DelayedInfoDTO;
import com.doubleysoft.delayquene4j.support.JsonProvider;
import com.doubleysoft.delayquene4j.support.LockProvider;
import com.doubleysoft.delayquene4j.support.RedisProvider;
import com.doubleysoft.delayquene4j.tasks.PullInTimeMsgTask;
import com.doubleysoft.delayquene4j.tasks.PullOutTimeMsgTask;
import com.doubleysoft.delayquene4j.tasks.RedisDelayMsgService;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author dongyang.yu
 * @email dongyang.yu@anxincloud.com
 */
@Slf4j
public class DelayMsgConfig implements DelayMsgService {
    private DelayedProperties delayedProperties;
    private LockProvider lockProvider;
    private RedisProvider redisProvider;
    private JsonProvider jsonProvider;
    private DelayMsgService delayMsgService;
    private int status;

    public DelayMsgConfig(RedisProvider redisProvider, LockProvider lockProvider,
                          JsonProvider jsonProvider, DelayedProperties delayedProperties) {
        this.redisProvider = redisProvider;
        this.lockProvider = lockProvider;
        this.jsonProvider = jsonProvider;
        this.delayedProperties = delayedProperties;
        this.delayMsgService = new RedisDelayMsgService(redisProvider, jsonProvider);
        this.beginTimerTasks();
    }

    @Override
    public void addDelayMessage(DelayedInfoDTO delayedInfoDTO, DelayedMsgHandler msgHandler) {
        check();
        this.delayMsgService.addDelayMessage(delayedInfoDTO, msgHandler);
    }

    @Override
    public void addDelayMessage(DelayedInfoDTO delayedInfoDTO) {
        check();
        this.delayMsgService.addDelayMessage(delayedInfoDTO);
    }

    @Override
    public void addDelayCallBack(String system, DelayedMsgHandler msgHandler) {
        check();
        this.delayMsgService.addDelayCallBack(system, msgHandler);
    }

    private void beginTimerTasks() {
        ExecutorService clientExecutorService = delayedProperties.getClientExecutorService();
        PullOutTimeMsgTask pullOutTimeMsgTask = new PullOutTimeMsgTask(lockProvider, redisProvider
                ,clientExecutorService, delayedProperties);
        PullInTimeMsgTask pullInTimeMsgTask = new PullInTimeMsgTask(redisProvider, jsonProvider
                ,clientExecutorService);
        status = 1;
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            pullInTimeMsgTask.stop();
            pullOutTimeMsgTask.stop();
        }));
    }

    private void check(){
        if (status != 1) {
            throw new RuntimeException("延迟队列初始化之前不能添加回调方法");
        }
    }
}
