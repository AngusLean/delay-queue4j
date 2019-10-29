package com.doubleysoft.delayquene4j;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;


@RequiredArgsConstructor
@Slf4j
public class DelayedMsgHandlerWrapper implements DelayedMsgHandler {
    private final DelayedMsgHandler delayedMsgHandler;

    @Override
    public void handle(String uuid, String message) {
        log.info("[Delay Queue] begin handle delay message:{}, {}", uuid, message);
        try {
            delayedMsgHandler.handle(uuid, message);
            log.info("[Delay Queue] complete handle delay message:{}, {}", uuid, message);
        } catch (Exception e) {
            log.warn("[Delay Queue] handle delay message:{} error", uuid, e);
        }
    }
}
