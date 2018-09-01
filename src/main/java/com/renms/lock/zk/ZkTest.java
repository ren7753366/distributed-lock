/**
 * Desc: zk lock test
 * FileName: ZkTesat
 * Author:   Administrator
 * Date:     2018/8/19 17:58
 */
package com.renms.lock.zk;

public class ZkTest {

    public static void main(String[] args) {

        Runnable runnable = new Runnable() {

            public void run() {

//                ZkExclusionLock lock = null;
//                try {
//                    lock = new ZkExclusionLock("10.182.19.194:2181","renms");
//                    boolean locked = lock.lock(1000);
//                    //获取锁成功，do someThing
//                    Thread.sleep(200L);
//                } catch (Exception e) {
//                    e.printStackTrace();
//                } finally {
//                    if (lock != null) {
//                        lock.unlock();
//                    }
//                }


                ZkShareLock lock = null;
                try {
                    lock = new ZkShareLock("10.182.19.194:2181","renms");
                    boolean locked = lock.readLock(1000);
                    //获取锁成功，do someThing
                    Thread.sleep(200L);
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    if (lock != null) {
                        lock.unlock();
                    }
                }

            }
        };

        Runnable writeRunnable = new Runnable() {

            public void run() {
                ZkShareLock lock = null;
                try {
                    lock = new ZkShareLock("10.182.19.194:2181","renms");
                    boolean locked = lock.writeLock(1000);
                    //获取锁成功，do someThing
                    Thread.sleep(100L);
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    if (lock != null) {
                        lock.unlock();
                    }
                }

            }
        };

        for (int i = 0; i < 3; i++) {

            Thread t2 = new Thread(writeRunnable);
            t2.setName("写锁线程--"+i);
            t2.start();
            Thread t = new Thread(runnable);
            t.setName("读锁线程--"+i);
            t.start();
        }
    }
}