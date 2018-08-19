/**
 * Desc: add redis lock
 * FileName: RedisLock
 * Author:   renms
 * Date:     2018/8/19 15:49
 */
package com.renms.lock.redis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisException;

import java.util.Collections;

public class RedisLock {

    private static final String LOCK_SUCCESS = "OK";
    private static final String SET_IF_NOT_EXIST = "NX";
    private static final String SET_WITH_EXPIRE_TIME = "PX";
    private final JedisPool jedisPool;

    public RedisLock(JedisPool jedisPool) {
        this.jedisPool = jedisPool;
    }


    public String lock(String lockName) {
        return lockWithTimeout(lockName,3,1000,5*60*1000);
    }
    /**
     * @param lockName     锁名称
     * @param acquireTimes 重试次数
     * @param acquireRate  重试频率
     * @param timeout      锁超时时间
     * @return
     */
    public String lockWithTimeout(String lockName, int acquireTimes, long acquireRate, long timeout) {
        Jedis conn = null;
        String retIdentifier = null;
        try {
            // 获取连接
            conn = jedisPool.getResource();
            String threadId = Thread.currentThread().getId()+"";
            // 锁名
            String lockKey = "lock_" + lockName;

            while (acquireTimes > 0) {
                acquireTimes--;

                String result = conn.set(lockKey, threadId, SET_IF_NOT_EXIST, SET_WITH_EXPIRE_TIME, timeout);
                if (LOCK_SUCCESS.equals(result)) {
                    return threadId;
                }
                try {
                    Thread.sleep(acquireRate);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        } catch (JedisException e) {
            e.printStackTrace();
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
        return null;
    }

    /**
     * 释放锁
     *
     * @param lockName   锁的key
     * @param identifier 释放锁的标识
     * @return
     */
    public boolean releaseLock(String lockName, String identifier) {
        if(identifier == null){
            return true;
        }
        Jedis conn = null;
        String lockKey = "lock_" + lockName;
        try {
            conn = jedisPool.getResource();

            String script = "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end";
            Object result = conn.eval(script, Collections.singletonList(lockKey), Collections.singletonList(identifier));

            if ("1".equals(result)) {
                return true;
            }
            return false;
        } catch (JedisException e) {
            e.printStackTrace();
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
        return false;
    }
}