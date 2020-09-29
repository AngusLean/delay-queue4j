package com.doubleysoft.delayquene4j;


public interface DelayedMsgHandler {
    void handle(String uuid, String message);
}
