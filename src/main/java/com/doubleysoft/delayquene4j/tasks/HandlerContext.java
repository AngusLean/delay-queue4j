package com.doubleysoft.delayquene4j.tasks;

import com.doubleysoft.delayquene4j.DelayedMsgHandler;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author dongyang.yu
 * @email dongyang.yu@anxincloud.com
 */
@Slf4j
class HandlerContext {
    private static final DelayedMsgHandler NO_OP_HANDLER = new NoOpDelayedMsgHandler();

    private static Map<String, DelayedMsgHandler> handlerMap = new ConcurrentHashMap<>();

    private static HandlerKeyChangeCallBack handlerKeyChangeCallBack;

    public static void addMsgHandler(String system, DelayedMsgHandler handler) {
        handlerMap.putIfAbsent(system, handler);
        if (handlerKeyChangeCallBack != null) {
            handlerKeyChangeCallBack.callBack(system);
        }
    }

    public static DelayedMsgHandler getMsgHandler(String system) {
        return handlerMap.getOrDefault(system, NO_OP_HANDLER);
    }

    public static Set<String> getHandlerKeys() {
        return handlerMap.keySet();
    }

    public static void setHandlerKeyChangeCallBack(HandlerKeyChangeCallBack callBack) {
        handlerKeyChangeCallBack = callBack;
    }

    static class NoOpDelayedMsgHandler implements DelayedMsgHandler {

        @Override
        public void handle(String uuid, String message) {
            log.warn("[Delay Queue]No Op delayed message handler handle message:{}, {}", uuid, message);
        }
    }

    interface HandlerKeyChangeCallBack {
        void callBack(String systemKey);
    }
}
