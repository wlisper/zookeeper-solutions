package com.example.zookeepersolutions.config;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.concurrent.CountDownLatch;

public class ConfigCenter implements Watcher {

    private ZooKeeper zooKeeper;
    private String configPath;
    private CountDownLatch countDownLatch;

    public ConfigCenter(String zookeeperHost, String configPath) throws Exception {
        this.zooKeeper = new ZooKeeper(zookeeperHost, 5000, this);
        this.configPath = configPath;
        this.countDownLatch = new CountDownLatch(1);
        ensurePathExists(configPath);
    }

    public void setConfig(String data) throws Exception {
        zooKeeper.setData(configPath, data.getBytes(), -1);
        System.out.println("Config updated: " + data);
    }

    public String getConfig() throws Exception {
        byte[] data = zooKeeper.getData(configPath, false, null);
        return new String(data);
    }

    private void ensurePathExists(String path) throws Exception {
        Stat stat = zooKeeper.exists(path, false);
        if (stat == null) {
            zooKeeper.create(path, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
    }

    @Override
    public void process(WatchedEvent event) {
        if (event.getType() == Event.EventType.NodeDataChanged && event.getPath().equals(configPath)) {
            try {
                byte[] data = zooKeeper.getData(configPath, false, null);
                // update local config.
                System.out.println("Config updated: " + new String(data));
                zooKeeper.exists(configPath, this);  // register watcher once again
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws Exception {
        String zookeeperHost = "localhost:2181";
        String configPath = "/config";

        ConfigCenter configCenter = new ConfigCenter(zookeeperHost, configPath);
        configCenter.setConfig("initial config");

        String config = configCenter.getConfig();
        System.out.println("Current config: " + config);

        // Update the config
        configCenter.setConfig("updated config");
        Thread.sleep(1000);

        config = configCenter.getConfig();
        System.out.println("Current config: " + config);
    }
}