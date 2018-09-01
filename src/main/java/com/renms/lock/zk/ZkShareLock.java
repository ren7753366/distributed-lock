/**
 * Desc: zk lock
 * FileName: ZkShareLock
 * Author:   renms
 * Date:     2018/8/19 17:10
 */
package com.renms.lock.zk;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ZkShareLock implements Watcher {

    private ZooKeeper zk;
    private String ROOT_LOCK = "/locks";    // 根节点
    private String SHARE_ROOT_LOCK = ROOT_LOCK + "/share_lock";   //排它锁节点
    private int sessionTimeout = 50000; //连接超时时间
    private CountDownLatch countDownLatch;
    private String myLockPath; //当前节点
    private String waitLockPath; //等待的锁节点
    private String lockName;


    /**
     * 创建zk锁,初始化跟节点
     *
     * @param config 连接配置   ip:port
     */
    public ZkShareLock(String config, String lockName) {
        try {
            this.lockName = lockName;
            // 连接zookeeper
            this.zk = new ZooKeeper(config, sessionTimeout, this);
            Stat stat = zk.exists(ROOT_LOCK, false);
            if (stat == null) {
                // 如果根节点不存在，则创建根节点
                zk.create(ROOT_LOCK, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }

            stat = zk.exists(SHARE_ROOT_LOCK, false);
            if (stat == null) {
                // 如果排他锁根节点不存在，则创建
                zk.create(SHARE_ROOT_LOCK, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
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
     * 无超时时间-一直等到锁为止（读锁）
     *
     * @return
     */
    public boolean readLock() {
        return readLock(-1);
    }

    /**
     * 无超时时间-一直等到锁为止(写锁)
     *
     * @return
     */
    public boolean writeLock() {
        return writeLock(-1);
    }

    /**
     * @param timeout 锁等待超时时间
     * @return
     */
    public boolean readLock(long timeout) {
        try {
            if (tryLock(LockType.READ, timeout)) {
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
     * @param timeout 锁等待超时时间
     * @return
     */
    public boolean writeLock(long timeout) {
        try {
            if (tryLock(LockType.WRITE, timeout)) {
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

    public boolean tryLock(LockType lockType, long timeout) throws InterruptedException {
        try {

            String split = "_zkslock_";
            //stodo:判断lockName 是否符合要求
            if (lockName.contains(split)) {
                throw new RuntimeException("lockName can not contains 【split】");
            }

            Long threadId = Thread.currentThread().getId();
            myLockPath = String.valueOf(zk.create(SHARE_ROOT_LOCK + "/" + lockName + split + lockType.name() + "_", new byte[]{threadId.byteValue()},
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL));

            //获取共享锁根目录下的所有节点
            List<String> subNodes = zk.getChildren(SHARE_ROOT_LOCK, false);

            //取出所有lockName的锁
            List<String> lockObjNodes = new ArrayList<String>();
            for (String node : subNodes) {
                String _node = node.split(split)[0];
                if (_node.equals(lockName)) {
                    lockObjNodes.add(node);
                }
            }
            Collections.sort(lockObjNodes);

            if (myLockPath.equals(SHARE_ROOT_LOCK + "/" + lockObjNodes.get(0))) {
                //如果是最小的节点,则表示取得锁
                System.out.println(myLockPath + "==" + lockObjNodes.get(0));
                return true;
            }
            if (String.valueOf(threadId).equals(String.valueOf(zk.getData(SHARE_ROOT_LOCK + "/" + lockObjNodes.get(0), false, null)))) {
                //锁重入
                System.out.println(myLockPath + " 重入" + threadId);
                return true;
            }
            //判断是否是写锁
            if (myLockPath.indexOf(split + LockType.WRITE) > 0) {
                //如果不是最小的节点，找到比自己小1的节点
                String subMyZnode = myLockPath.substring(myLockPath.lastIndexOf("/") + 1);
                waitLockPath = lockObjNodes.get(Collections.binarySearch(lockObjNodes, subMyZnode) - 1);//找到前一个子节点
            } else {
                //读锁
                String subMyZnode = myLockPath.substring(myLockPath.lastIndexOf("/") + 1);
                int idex= Collections.binarySearch(lockObjNodes, subMyZnode);
                while (idex>0) {
                    idex--;
                    String path = lockObjNodes.get(idex);
                    if(path.indexOf(split + LockType.WRITE) > 0){
                        waitLockPath = path;
                        break;
                    }
                }
                if(waitLockPath == null) {
                    return true;
                }
            }
            return waitForLock(waitLockPath, timeout);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    private boolean waitForLock(String lower, long waitTime) throws InterruptedException, KeeperException {
        Stat stat = zk.exists(SHARE_ROOT_LOCK + "/" + lower, true);//同时注册监听。
        //判断比自己小一个数的节点是否存在,如果不存在则无需等待锁,同时注册监听
        if (stat != null) {
            System.out.println(Thread.currentThread().getName() + " 等待锁  " + SHARE_ROOT_LOCK + "/" + lower);
            long start = System.currentTimeMillis();
            this.countDownLatch = new CountDownLatch(1);
            this.countDownLatch.await(waitTime, TimeUnit.MILLISECONDS);//等待，这里应该一直等待其他线程释放锁
            this.countDownLatch = null;
            //判断是否锁等待超时
            long end = System.currentTimeMillis();
            long waiting = end - start;
            if (waitTime < waiting) {
                System.out.println("锁等待超时 waitTime:" + waitTime + ",lockName:" + lockName);
                return false;
            }
        }
        return true;
    }

    public void unlock() {
        if (myLockPath == null) {
            return;
        }
        try {
            System.out.println(Thread.currentThread().getName() + "释放锁 " + myLockPath);
            zk.delete(myLockPath, -1);
            myLockPath = null;
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
