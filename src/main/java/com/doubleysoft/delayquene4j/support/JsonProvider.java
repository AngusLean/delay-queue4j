package com.doubleysoft.delayquene4j.support;

/**
 * @author dongyang.yu
 * @email dongyang.yu@anxincloud.com
 */
public interface JsonProvider {

    String toJSONString(Object obj);

    <T> T fromJSONString(String jsonString, Class<T> clazz);
}
