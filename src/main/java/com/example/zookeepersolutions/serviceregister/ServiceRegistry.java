package com.example.zookeepersolutions.serviceregister;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;

public class ServiceRegistry implements Watcher {

    private ZooKeeper zooKeeper;
    private String registryPath;

    public ServiceRegistry(String zookeeperHost, String registryPath) throws Exception {
        this.zooKeeper = new ZooKeeper(zookeeperHost, 5000, this);
        this.registryPath = registryPath;
        ensurePathExists(registryPath);
    }

    public void registerService(String serviceName, int servicePort) throws Exception {
        String serviceNodePath = registryPath + "/" + serviceName;
        String serviceAddress = getAddress() + ":" + servicePort;
        String serviceNode = zooKeeper.create(serviceNodePath, serviceAddress.getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        System.out.println("Registered service: " + serviceNode);
    }

    public List<String> getServiceAddresses(String serviceName) throws Exception {
        String serviceNodePath = registryPath + "/" + serviceName;
        List<String> addresses = zooKeeper.getChildren(serviceNodePath, false);
        List<String> serviceAddresses = new ArrayList<>();
        for (String address : addresses) {
            String addressPath = serviceNodePath + "/" + address;
            byte[] data = zooKeeper.getData(addressPath, false, null);
            serviceAddresses.add(new String(data));
        }
        return serviceAddresses;
    }

    private void ensurePathExists(String path) throws Exception {
        Stat stat = zooKeeper.exists(path, false);
        if (stat == null) {
            zooKeeper.create(path, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
    }

    private String getAddress() {
        try {
            return InetAddress.getLocalHost().getHostAddress();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void process(WatchedEvent event) {
        // Handle events if needed
    }

    public static void main(String[] args) throws Exception {
        String zookeeperHost = "localhost:2181";
        String registryPath = "/services";
        ServiceRegistry serviceRegistry = new ServiceRegistry(zookeeperHost, registryPath);
        serviceRegistry.registerService("service1", 8080);
        serviceRegistry.registerService("service2", 8081);

        List<String> service1Addresses = serviceRegistry.getServiceAddresses("service1");
        System.out.println("Service1 addresses: " + service1Addresses);

        List<String> service2Addresses = serviceRegistry.getServiceAddresses("service2");
        System.out.println("Service2 addresses: " + service2Addresses);
    }

}