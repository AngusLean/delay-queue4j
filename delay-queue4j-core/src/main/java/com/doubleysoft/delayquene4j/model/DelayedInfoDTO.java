package com.doubleysoft.delayquene4j.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;


@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DelayedInfoDTO {
    //delay time, seconds
    private Long delayTime;

    //different system will be stored in different queue.
    private String system;

    //uuid
    private String uuid;

    //actual message which delaymessagehandle will receive
    private String message;

    private Long timestamp;
}
