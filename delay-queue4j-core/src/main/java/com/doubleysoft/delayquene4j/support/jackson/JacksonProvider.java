package com.doubleysoft.delayquene4j.support.jackson;

import com.doubleysoft.delayquene4j.support.JsonProvider;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;

/**
 * @author dongyang.yu
 * @email dongyang.yu@anxincloud.com
 */
public class JacksonProvider implements JsonProvider {
    private ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public String toJSONString(Object obj) {
        try {
            return objectMapper.writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    @Override
    public <T> T fromJSONString(String jsonString, Class<T> clazz) {
        try {
            return objectMapper.readValue(jsonString, clazz);
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage());
        }
    }
}
