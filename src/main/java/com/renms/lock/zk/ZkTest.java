/**
 * Desc: zk lock test
 * FileName: ZkTesat
 * Author:   Administrator
 * Date:     2018/8/19 17:58
 */
package com.renms.lock.zk;

import java.util.Random;

public class ZkTest {

    public static void main(String[] args) {



        Runnable runnable = new Runnable() {

            public void run() {

                //随机创建读锁和写锁
                String lockType = ZkLock.READ_LOCK;
                int value = new Random().nextInt(1000);
                if(value%2==0){
                    lockType = ZkLock.WRIET_LOCK;
                }

                ZkLock lock = null;
                try {
                    lock = new ZkLock("127.0.0.1:2181", "test1"+lockType);
                    lock.lock();
                    //获取锁成功，do someThing

                } finally {
                    if (lock != null) {
                        lock.unlock();
                    }
                }
            }
        };

        for (int i = 0; i < 3; i++) {
            Thread t = new Thread(runnable);
            t.start();
        }
    }
}