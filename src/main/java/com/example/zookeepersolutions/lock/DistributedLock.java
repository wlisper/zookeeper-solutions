package com.example.zookeepersolutions.lock;

import org.apache.zookeeper.*;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * implement distributed lock with zookeeper
 */
public class DistributedLock implements Watcher {

    private ZooKeeper zooKeeper;
    private String lockPath;
    private String currentPath;
    private String waitPath;
    private CountDownLatch countDownLatch;

    public DistributedLock(String zookeeperHost, String lockPath) throws Exception {
        this.zooKeeper = new ZooKeeper(zookeeperHost, 5000, this);
        this.lockPath = lockPath;
        this.currentPath = this.zooKeeper.create(lockPath + "/lock_", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        this.countDownLatch = new CountDownLatch(1);
    }

    public void lock() throws Exception {
        List<String> children = zooKeeper.getChildren(lockPath, false);
        Collections.sort(children);

        if (currentPath.equals(lockPath + "/" + children.get(0))) {
            System.out.println("Acquired lock");
            return;
        }

        int currentPathIndex = children.indexOf(currentPath.substring(lockPath.length() + 1));
        if (currentPathIndex > 0) {
            waitPath = lockPath + "/" + children.get(currentPathIndex - 1);
            zooKeeper.exists(waitPath, true);   // watch this path
            countDownLatch.await();
            System.out.println("Acquired lock");
        }
    }

    public void unlock() throws Exception {
        zooKeeper.delete(currentPath, -1);
        zooKeeper.close();
        System.out.println("Released lock");
    }

    @Override
    public void process(WatchedEvent event) {
        if (event.getType() == Event.EventType.NodeDeleted && event.getPath().equals(waitPath)) {
            countDownLatch.countDown();
        }
    }

    public static void main(String[] args) throws Exception {
        String zookeeperHost = "localhost:2181";
        String lockPath = "/distributed-lock";

        DistributedLock lock = new DistributedLock(zookeeperHost, lockPath);
        lock.lock();

        // 执行临界区代码

        lock.unlock();
    }

}
