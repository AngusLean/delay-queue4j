package com.doubleysoft.delayquene4j;

import com.doubleysoft.delayquene4j.model.DelayedInfoDTO;


public interface DelayMsgService {
    /**
     * add delay message to queue
     *
     * @param delayedInfoDTO
     */
    void addDelayMessage(DelayedInfoDTO delayedInfoDTO, DelayedMsgHandler msgHandler);

    void addDelayMessage(DelayedInfoDTO delayedInfoDTO);

    void addDelayCallBack(String system, DelayedMsgHandler msgHandler);
}
