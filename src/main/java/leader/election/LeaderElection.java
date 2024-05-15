package leader.election;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class LeaderElection implements Watcher {

	private static final String ZOOKEEPER_ADDRESS = "34.228.23.62:80"; // endereço do ZooServer
	private static final int TIMEOUT = 3000;
	private static final String ELECTION_NAMESPACE = "/election";
	private ZooKeeper zooKeeper;
	private String currentZNodeName;

	public static void main(String[] args) throws IOException, KeeperException, InterruptedException {

		LeaderElection leaderElection = new LeaderElection();

		leaderElection.connectToZookeeper();
		leaderElection.volunteerForLeadership();
		leaderElection.reelectLeader();
		leaderElection.run();
		leaderElection.close();
		
		System.out.println("****** Disconected from ZooKeper! Exiting application... bye ****");
	}

	public void connectToZookeeper() throws IOException {
		this.zooKeeper = new ZooKeeper(ZOOKEEPER_ADDRESS, TIMEOUT, null);
	}

	public void volunteerForLeadership() throws KeeperException, InterruptedException {
		String znodePrefix = ELECTION_NAMESPACE + "/c_";
		String znodeFullPath = zooKeeper.create(znodePrefix, new byte[] {}, ZooDefs.Ids.OPEN_ACL_UNSAFE,
				CreateMode.EPHEMERAL_SEQUENTIAL);

		System.out.println("\n*****My Name is: " + znodeFullPath + " *****");
		this.currentZNodeName = znodeFullPath.replace(ELECTION_NAMESPACE + "/", "");
	}

	public void reelectLeader() throws KeeperException, InterruptedException {
		Stat predecessorStat = null;
		String predecessorZnodeName = "";
		while (predecessorStat == null) {
			List<String> children = zooKeeper.getChildren(ELECTION_NAMESPACE, false);
			Collections.sort(children);
			String smallestChild = children.get(0);

			if (smallestChild.equals(currentZNodeName)) {
				System.out.println("\n***** I'm the leader!! *****");
				return;
			} else {
				System.out.println("\n***** I'm not the leader! *****");
				int predecessorIndex = Collections.binarySearch(children, currentZNodeName) - 1;
				predecessorZnodeName = children.get(predecessorIndex);
				predecessorStat = zooKeeper.exists(ELECTION_NAMESPACE + "/" + predecessorZnodeName, this);
			}
		}
		System.out.println("\n****** Watching Znode " + predecessorZnodeName + "******\n");
	}

	public void run() throws InterruptedException {
		synchronized (zooKeeper) {
			zooKeeper.wait();
		}
	}

	public void close() throws InterruptedException {
		zooKeeper.close();
	}

	@Override
	public void process(WatchedEvent event) {
		switch (event.getType()) {
		case None:
			if (event.getState() == Event.KeeperState.SyncConnected) {
				System.out.println("\n***** Successfully connected to Zookeeper! *****");
			} else {
				synchronized (zooKeeper) {
					System.out.println("\n***** Disconected from ZooKeeper! *****");
					zooKeeper.notifyAll();
				}
			}
			break;
		case NodeDeleted:
			try {
				reelectLeader();
			} catch (InterruptedException e) {
			} catch (KeeperException e) {
				System.err.println("*** ERROR: " + e.getMessage());
			}

			break;
		default:
			break;
		}
	}

}
