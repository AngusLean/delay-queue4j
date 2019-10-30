package com.doubleysoft.delayquene4j.support.reddison;

import com.doubleysoft.delayquene4j.support.LockProvider;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;

import java.util.concurrent.TimeUnit;

/**
 * @author dongyang.yu
 * @email dongyang.yu@anxincloud.com
 */
@Slf4j
@RequiredArgsConstructor
public class RedissonRedisLockProvider implements LockProvider {
    private final RedissonClient redissonClient;

    @Override
    public boolean lock(String key, Long timeOut) {
        RLock lock = redissonClient.getLock(key);
        lock.lock(timeOut, TimeUnit.SECONDS);
        return true;
    }

    @Override
    public void release(String key) {
        RLock lock = redissonClient.getLock(key);
        if (lock == null || !lock.isHeldByCurrentThread()) {
            return;
        }
        lock.forceUnlock();
    }
}
