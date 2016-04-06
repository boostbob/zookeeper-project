package com.yanhe;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Perms;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.server.auth.DigestAuthenticationProvider;

public class CreateNodeSyncAuth implements Watcher {
	private static ZooKeeper zookeeper;
	private static boolean somethingDone = false;

	public static void main(String[] args) throws IOException, InterruptedException {
		zookeeper = new ZooKeeper("10.211.55.7:2181", 5000, new CreateNodeSyncAuth());
		System.out.println(zookeeper.getState());

		Thread.sleep(Integer.MAX_VALUE);
	}

	private void doSomething() {
		try {
			ACL aclIp = new ACL(Perms.READ, new Id("ip", "192.168.1.105"));
			ACL aclDigest = new ACL(Perms.READ | Perms.WRITE,
					new Id("digest", DigestAuthenticationProvider.generateDigest("jike:123456")));
			ArrayList<ACL> acls = new ArrayList<ACL>();
			acls.add(aclDigest);
			acls.add(aclIp);
			
			// 发送授权信息
			// zookeeper.addAuthInfo("digest", "jike:123456".getBytes());
			String path = zookeeper.create("/node_4", "123".getBytes(), acls, CreateMode.PERSISTENT);
			System.out.println("return path:" + path);

			// 网络不稳定的时候回多次重连，该函数可能多次被调用
			somethingDone = true;
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void process(WatchedEvent event) {
		System.out.println("Event Args: " + event);
		
		if (event.getState() == KeeperState.SyncConnected) {
			if (!somethingDone && event.getType() == EventType.None && null == event.getPath()) {
				doSomething();
			}
		}
	}
}
