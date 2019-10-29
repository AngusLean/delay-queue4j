package com.doubleysoft.delayquene4j;

/**
 * @author dongyang.yu
 * @email dongyang.yu@anxincloud.com
 */
public interface LockProvider {
    boolean lock(String key, Long timeOut);

    void release(String key);
}
