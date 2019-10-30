package com.doubleysoft.delayquene4j.tasks;

import com.doubleysoft.delayquene4j.DelayedMsgHandler;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author dongyang.yu
 * @email dongyang.yu@anxincloud.com
 */
@Slf4j
class HandlerContext {
    private static final DelayedMsgHandler NO_OP_HANDLER = new NoOpDelayedMsgHandler();

    private static Map<String, DelayedMsgHandler> handlerMap = new ConcurrentHashMap<>();

    public static void addMsgHandler(String system, DelayedMsgHandler handler) {
        handlerMap.putIfAbsent(system, handler);
    }

    public static DelayedMsgHandler getMsgHandler(String system) {
        return handlerMap.getOrDefault(system, NO_OP_HANDLER);
    }

    static class NoOpDelayedMsgHandler implements DelayedMsgHandler {

        @Override
        public void handle(String uuid, String message) {
            log.warn("[Delay Queue]No Op delayed message handler handle message:{}, {}", uuid, message);
        }
    }
}
