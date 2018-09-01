/**
 * Desc: zk lock
 * FileName: ZkExclusionLock
 * Author:   renms
 * Date:     2018/8/19 17:10
 */
package com.renms.lock.zk;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ZkExclusionLock2 implements Watcher {

    private ZooKeeper zk;
    private String ROOT_LOCK = "/locks";    // 根节点
    private String EXCLUSION_ROOT_LOCK = ROOT_LOCK + "/exclusion_lock";   //排它锁节点
    private int sessionTimeout = 50000; //连接超时时间
    private CountDownLatch countDownLatch;
    private String currentLockPath; //当前节点
    private String lockName;


    /**
     * 创建zk锁,初始化跟节点
     *
     * @param config 连接配置   ip:port
     */
    public ZkExclusionLock2(String config, String lockName) {
        try {
            this.lockName = lockName;
            // 连接zookeeper
            this.zk = new ZooKeeper(config, sessionTimeout, this);
            Stat stat = zk.exists(ROOT_LOCK, false);
            if (stat == null) {
                // 如果根节点不存在，则创建根节点
                zk.create(ROOT_LOCK, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }

            stat = zk.exists(EXCLUSION_ROOT_LOCK, false);
            if (stat == null) {
                // 如果排他锁根节点不存在，则创建
                zk.create(EXCLUSION_ROOT_LOCK, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }

        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }

    /**
     * 无超时时间-一直等到锁为止
     *
     * @return
     */
    public boolean lock() {
        try {
            if (tryLock(-1)) {
                System.out.println(Thread.currentThread().getName() + "获取锁");
                return true;
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     *
     * @param timeout  锁等待超时时间
     * @return
     */
    public boolean lock(long timeout) {
        try {
            if (tryLock(timeout)) {
                System.out.println(Thread.currentThread().getName() + "获取锁");
                return true;
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    public boolean tryLock(long timeout) throws InterruptedException {
        try {
            Stat stat = zk.exists(EXCLUSION_ROOT_LOCK + "/" + lockName, false);
            if (stat != null) {
                String data = String.valueOf(zk.getData(EXCLUSION_ROOT_LOCK + "/" + lockName, false, stat));
                if (String.valueOf(Thread.currentThread().getId()).equals(data)) {
                    return true;//重入锁
                }
            } else {
                Long id = Thread.currentThread().getId();

                try {
                    //尝试创建锁
                    currentLockPath = zk.create(EXCLUSION_ROOT_LOCK + "/" + lockName, new byte[]{id.byteValue()},
                            ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                } catch (KeeperException.NodeExistsException e) {
                    currentLockPath = null;
                }
                if (currentLockPath != null) {
                    return true;
                }
            }
            return waitForLock(timeout);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    // 等待锁
    private boolean waitForLock(long waitTime) throws KeeperException, InterruptedException {
        Stat stat = zk.exists(EXCLUSION_ROOT_LOCK + "/" + lockName, true);
        if (stat != null) {
            System.out.println(Thread.currentThread().getName() + "等待锁 " + EXCLUSION_ROOT_LOCK + "/" + lockName);
            this.countDownLatch = new CountDownLatch(1);
            // 计数等待，若等到前一个节点消失，则precess中进行countDown，停止等待，获取锁
            long start = System.currentTimeMillis();

            if (waitTime <= 0) {
                this.countDownLatch.await();
            }else{
                this.countDownLatch.await(waitTime, TimeUnit.MILLISECONDS);
                //重新竞争锁资源
                long end = System.currentTimeMillis();
                long waiting = end - start;
                if (waitTime < waiting) {
                    System.out.println("锁等待超时 waitTime:"+waitTime+",lockName:"+lockName);
                    return false;
                }
            }
            System.out.println("唤醒线程：" + Thread.currentThread().getId());
        }
        return tryLock(waitTime);
    }

    public void unlock() {
        if (currentLockPath == null) {
            return;
        }
        try {
            System.out.println(Thread.currentThread().getName() + "释放锁 " + currentLockPath);
            zk.delete(currentLockPath, -1);
            currentLockPath = null;
            zk.close();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        } finally {

        }
    }

    public void process(WatchedEvent event) {
        if (this.countDownLatch != null) {
            this.countDownLatch.countDown();
        }
    }
}
