/**
 * Desc: redis lock test
 * FileName: RedisTest
 * Author:   renms
 * Date:     2018/8/19 16:49
 */
package com.renms.lock.redis;

import redis.clients.jedis.JedisPool;

public class RedisTest {

    public static void main(String[] args) {
        for (int i = 0; i < 10; i++) {
            MyThread thread = new MyThread("分布式锁线程_" + i);
            thread.start();
        }
    }

    public static class MyThread extends Thread {
        JedisPool jp = null;

        public MyThread(String name) {
            this.setName(name);
            jp = new JedisPool("127.0.0.1", 6379);
        }

        @Override
        public void run() {
            String value = null;
            RedisLock lock = new RedisLock(jp);
            try {
                value = lock.lock("myFirstLock");

                if (value != null) {
                    System.out.println(this.getName() + " 获取锁成功 value=" + value);

                    Thread.sleep(100);

                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                if (value != null) {
                    lock.releaseLock("myFirstLock", value);
                }
            }
        }
    }


}
