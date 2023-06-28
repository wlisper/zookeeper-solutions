package com.example.zookeepersolutions.queue;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * implement distributed queue with zookeeper
 */
public class DistributedQueue implements Watcher {

    private ZooKeeper zooKeeper;
    private String queuePath;
    private CountDownLatch countDownLatch;

    public DistributedQueue(String zookeeperHost, String queuePath) throws Exception {
        this.zooKeeper = new ZooKeeper(zookeeperHost, 5000, this);
        this.queuePath = queuePath;
        this.countDownLatch = new CountDownLatch(1);
        ensurePathExists(queuePath);
    }

    public void offer(String data) throws Exception {
        String nodePath = zooKeeper.create(queuePath + "/node_", data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
        System.out.println("Offered: " + data);
        System.out.println("Node Path: " + nodePath);
    }

    public String poll() throws Exception {
        while (true) {
            List<String> children = zooKeeper.getChildren(queuePath, false);
            if (children.isEmpty()) {
                countDownLatch.await();
            } else {
                String firstNode = getFirstNode(children);
                String nodePath = queuePath + "/" + firstNode;
                byte[] data = zooKeeper.getData(nodePath, false, null);
                zooKeeper.delete(nodePath, -1);
                System.out.println("Polled: " + new String(data));
                return new String(data);
            }
        }
    }

    private void ensurePathExists(String path) throws Exception {
        Stat stat = zooKeeper.exists(path, false);
        if (stat == null) {
            zooKeeper.create(path, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
    }

    private String getFirstNode(List<String> nodes) {
        Collections.sort(nodes);
        return nodes.get(0);
    }

    @Override
    public void process(WatchedEvent event) {
        if (event.getType() == Event.EventType.NodeChildrenChanged && event.getPath().equals(queuePath)) {
            countDownLatch.countDown();
        }
    }

    public static void main(String[] args) throws Exception {
        String zookeeperHost = "localhost:2181";
        String queuePath = "/distributed-queue";

        DistributedQueue queue = new DistributedQueue(zookeeperHost, queuePath);
        queue.offer("data1");
        queue.offer("data2");

        String data = queue.poll();
        System.out.println("Polled data: " + data);
    }
}
