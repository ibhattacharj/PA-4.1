package server.faulttolerance;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import edu.umass.cs.nio.interfaces.NodeConfig;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.nio.nioutils.NodeConfigUtils;
import edu.umass.cs.utils.Util;
import org.apache.zookeeper.*;
import server.ReplicatedServer;
import server.SingleServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class MyDBFaultTolerantServerZK extends server.MyDBSingleServer {

	public static final int SLEEP = 200;
	public static final boolean DROP_TABLES_AFTER_TESTS = true;
	public static final int MAX_LOG_SIZE = 400;
	public static final int DEFAULT_PORT = 2181;

	// need a single shared location that all replicas agree to write to or else eg test 33 will fail
	// we need an "overall folder" and a "folder" within it that has the ordered req log
	private static final String ZK_SHARED_LOCATION = "/zk/shared";
	private static final String ZK_LOGS = ZK_SHARED_LOCATION + "/log";

	// progress + checkpoint metadata 
	private static final String ZK_PROGRESS = ZK_SHARED_LOCATION + "/progress";
	private static final String ZK_CHECKPOINT = ZK_SHARED_LOCATION + "/checkpoint";

	// truncation tuning
	private static final int TRUNCATE_SAFETY_MARGIN = 150; // keep a few extra for safety
	private static final int TRUNCATE_EVERY_N_APPLIES = 50; // avoid hammering ZK

	private final String myID;
	private final InetSocketAddress isaDB;
	private final List<String> allReplicaIDs;

	private ZooKeeper zk;
	private Cluster cluster;
	private Session session;

	// each server needs local mem of the highest log seq # its alr executed on its cassandra copy
	// applier thread may increment at the same time client thread is reading --> atomic deals w/ this
	private final AtomicLong lastApplied = new AtomicLong(-1);
	private final AtomicLong appliedSinceTruncate = new AtomicLong(0);

	// helps make replication happen; runs a thread that scans zk log and applies next entries
	// client reqs come in at rand times --> need a continuously running mechanism that notices new log entries and applies them in order
	private final ExecutorService applierExec = Executors.newSingleThreadExecutor();
	private volatile boolean running = true;

	private volatile boolean recentlyRecovered = false;

	// can't immediately reply when req arrives; could break linearizability --> only server that accepted a client req needs to keep track and reply
	// applier thread and client-handling thread may touch at same time --> avoid race conditions
	private final ConcurrentHashMap<Long, InetSocketAddress> pendingReplies = new ConcurrentHashMap<>();

	// OVERALL IDEA
	// one shared ZooKeeper log --> /dbft/mydbft/log/req-0000000000, /req-0000000001, ...
	// every client req becomes a new znode in that log (globally ordered by the seq number zk assigns)
	// every server runs the same background loop:
		// read list of znodes in /log
		// apply the next one it hasn’t applied yet (lastApplied + 1)
		// repeat
		// so even if reqs arrive at diff servers, all servers still execute them in the same order bc they all replay the same zk log
	public MyDBFaultTolerantServerZK(NodeConfig<String> nodeConfig, String myID, InetSocketAddress isaDB)
			throws IOException {
		super(new InetSocketAddress(nodeConfig.getNodeAddress(myID),
				nodeConfig.getNodePort(myID) - ReplicatedServer.SERVER_PORT_OFFSET),
				isaDB, myID);

		this.myID = myID;
		this.isaDB = isaDB;

		// keeping list of replicas for safe truncation
		this.allReplicaIDs = new java.util.ArrayList<>(nodeConfig.getNodeIDs());

		setup();
		recoverFromZKLog();
		startApplyLoop();
	}

	@Override
	protected void handleMessageFromClient(byte[] bytes, NIOHeader header) {

		// is only running on server that gets client req, so if reqs arrive at other servers applies, immediately crashes and restarts
		// concurrency (eg client A wants to add 1 to server 0 + client B wants to add 4 to server 2) --> not linearizable if applied immediately bc could be [0,2] or [2,0]
		// that's why no applying log entries here; needs to be done w/ ExecutorService thread

		String payload = new String(bytes, StandardCharsets.UTF_8);

		// append request to the shared ZK log
		long seq = appendRequest(payload);

		// remember where to reply once this server applies seq
		pendingReplies.put(seq, header.sndr);

		// small bounded wait for responsiveness; applier will respond eventually either way
		waitUntilApplied(seq, 10_000);
	}

	// @Override
	// protected void handleMessageFromServer(byte[] bytes, NIOHeader header) {
	// }

	@Override
	public void close() {
		running = false;
		applierExec.shutdownNow();
		try {
			if (zk != null)
				zk.close();
		} catch (InterruptedException ie) {
			Thread.currentThread().interrupt();
		}
		try {
			if (session != null)
				session.close();
		} catch (Exception ignored) {
		}
		try {
			if (cluster != null)
				cluster.close();
		} catch (Exception ignored) {
		}
		super.close();
	}

	public static enum CheckpointRecovery {
		CHECKPOINT, RESTORE;
	}

	private void setup() throws IOException {
		cluster = Cluster.builder().addContactPoint(isaDB.getHostString()).withPort(isaDB.getPort()).build();
		session = cluster.connect(myID);

		// zk connects async; new zk insantiation returns immediately before connection
		// is usable, and we will get error if we try to create znodes too early
		// latch lets us pause until ZK is acc connected
		CountDownLatch connected = new CountDownLatch(1);

		// connects to zk at localhost:2181, sets session timeout of 10 seconds, watcher
		// callback
		zk = new ZooKeeper("localhost:" + DEFAULT_PORT, 10_000, e -> {
			if (e.getState() == Watcher.Event.KeeperState.SyncConnected)
				connected.countDown();
		});

		try {
			// blocks the constructor thread until either zk successfully connects (good) or
			// 10 seconds pass (bad)
			connected.await(10, TimeUnit.SECONDS);
		} catch (InterruptedException ie) {
			Thread.currentThread().interrupt();
		}

		// making sure paths exist
		ensurePath("/zk");
		ensurePath(ZK_SHARED_LOCATION);
		ensurePath(ZK_LOGS);
		ensurePath(ZK_PROGRESS);
		ensurePath(ZK_CHECKPOINT);

		lastApplied.set(-1);
	}

	private void ensurePath(String fullPath) {
		if (fullPath == null || fullPath.isEmpty() || "/".equals(fullPath))
			return;
		// ensure that a full zookeeper path exists by creating any missing parents
		// eg "/zk/shared/log" --> creates "/zk", then "/zk/shared", then "/zk/shared/log"
		try {
			StringBuilder cur = new StringBuilder();
			for (String p : fullPath.split("/")) {
				if (p.isEmpty())
					continue; // skip leading or repeated '/'
				cur.append('/').append(p);
				String path = cur.toString();
				if (zk.exists(path, false) != null)
					continue;
				try {
					zk.create(path, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
				} 
				catch (KeeperException.NodeExistsException ignored) {
					// multiple replicas may race to create the same path; safe to ignore
				}
			}
		} 
		catch (Exception e) {
			throw new RuntimeException("Failed to ensure ZK path: " + fullPath, e);
		}
	}

	private void writeZKMetadata(String path, byte[] bytes) {
		// write small metadata to zk at a known path
		// used for things like:
		// - server's progress (highest applied log seq)
		// - checkpoint/truncation metadata
		// NOT used for storing acc reqs or large data
		// create the node if it doesnt exist yet, otherwise overwrite it; races expected
		try {
			if (zk.exists(path, false) == null) {
				try {
					zk.create(path, bytes, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
					return;
				} catch (KeeperException.NodeExistsException ignored) {
				}
			}
			zk.setData(path, bytes, -1);
		} catch (Exception ignored) {
		}
	}

	// client --> zk
	private long appendRequest(String payload) {
		try {
			// PERSISTENT_SEQUENTIAL --> create a node named with "req-" plus a globally increasing sequence number
			// guarantees that seq nums are unique + totally ordered for that path
			// nodes kept even if the client disconnects

			// zk.create --> full path of the newly created node
			String created = zk.create(ZK_LOGS + "/req-",
					payload.getBytes(StandardCharsets.UTF_8),
					ZooDefs.Ids.OPEN_ACL_UNSAFE,
					CreateMode.PERSISTENT_SEQUENTIAL);
			return parseSeq(created);
		} catch (Exception e) {
			throw new RuntimeException("Failed to append to ZK log", e);
		}
	}

	private static long parseSeq(String s) {
		// znode path ends w -<digits>, so get to last "-" and pull out num (eg
		// 0000000012 --> 12)
		int dash = s.lastIndexOf('-');
		if (dash < 0)
			throw new IllegalArgumentException("Bad znode path: " + s);
		return Long.parseLong(s.substring(dash + 1));
	}

	private static String childForSeq(List<String> children, long seq) {
		// given sorted list of child names finds the one w seq
		for (String c : children) {
			long s = parseSeq(c);
			if (s == seq)
				return c;
			if (s > seq)
				// if child seq > target then we skipped one; missing the next one we need
				// dont wanna break order so we'lll retry later
				return null;
		}
		return null;
	}

	// zk --> cassandra
	private void startApplyLoop() {
		applierExec.submit(() -> {
			while (running) {
				try {
					applyNextLogEntries();
					Thread.sleep(SLEEP);
				} catch (InterruptedException ie) {
					Thread.currentThread().interrupt();
					break;
				} catch (Exception ignored) {
				}
			}
		});
	}

	// apply as many zk log entries as we can rn
		// clients only append to zk (ordering happens there)
		// this loop replays that ordered log into cassandra
		// we only reply to clients after req is applied (linearizability)
	private void applyNextLogEntries() throws KeeperException, InterruptedException {
		// grab current ordered req log from zk
		// watch=true so zk tells us when new log entries are added
		List<String> children = zk.getChildren(ZK_LOGS, true);
		if (children.isEmpty())
			return;
		children.sort(Comparator.comparingLong(MyDBFaultTolerantServerZK::parseSeq));

		long oldest = parseSeq(children.get(0)), newest = parseSeq(children.get(children.size() - 1));
		long cur = lastApplied.get();

		// Debug: print state occasionally (every ~20 applies)
		if (cur % 20 == 0 || cur < 10)
			System.out.println(myID + " apply loop: lastApplied=" + cur + ", log=[" + oldest + ".." + newest + "] (" + children.size() + " entries)");

		long next = cur + 1;
		// if we restarted and log was truncated, "next" might not exist anymore
		// eg: lastApplied=-1 but oldest log entry = 205 --> stuck forever trying to apply 0 (but 0 - 204 dont exist anymore)
		// if behind the log, jump forward to the first available entry (eg 205)
		if (next < oldest) {
			// We're behind the log, skip to oldest available entry
			System.out.println(myID + " SKIPPING: wanted " + next + " but oldest available is " + oldest + " (gap of " + (oldest - next) + " entries)");
			lastApplied.set(oldest - 1);
			next = oldest;
		}

		// apply consecutive entries until gap or no more work
		while (true) {
			// find the znode where seq == next
			// returns null if not present yet (or got truncated/race); stop and retry later
			String child = childForSeq(children, next);
			if (child == null)
				return;

			// read znode data which is raw cql str
			String cql = new String(zk.getData(ZK_LOGS + "/" + child, false, null), StandardCharsets.UTF_8);

			// apply to cassandra before we advance lastApplied
			// lastApplied --> "this seq is def reflected in my cassandra state"
			String response = applyToCassandra(cql);
			long appliedSeq = lastApplied.incrementAndGet();

			// write this replica's progress to zk so truncation can be safe
			// other replicas will read /progress/* to compute minApplied
			writeZKMetadata(ZK_PROGRESS + "/" + myID, Long.toString(appliedSeq).getBytes(StandardCharsets.UTF_8));
			
			// bounded log requirement: truncate old log entries once all have applied them
			// but also delay truncation during recovery to avoid deleting stuff other replicas still need
			truncateIfNeeded();

			// only if this server is supposed to respond to the client who sent ths req
			// remove from map --> no dupes
			InetSocketAddress clientAddr = pendingReplies.remove(next);
			if (clientAddr != null)
				sendResponse(clientAddr, response);

			// make sure we don’t store too many pending replies
			// if a client disappears before we reply, we don’t need to keep its req forever --> remove old entries to keep memory usage bounded (max_log)
			if (pendingReplies.size() > MAX_LOG_SIZE) {
				long cutoff = next - MAX_LOG_SIZE;
				pendingReplies.keySet().removeIf(s -> s <= cutoff);
			}

			next = lastApplied.get() + 1;
		}
	}

	private String applyToCassandra(String cql) {
		try {
			ResultSet rs = session.execute(cql);
			// for queries like inserts/updates that dont show rows
			if (rs.getColumnDefinitions() == null || rs.getColumnDefinitions().size() == 0)
				return "OK";
			return "ROW_COUNT:" + rs.all().size();
		} catch (Exception e) {
			return "ERROR:" + e.getMessage();
		}
	}

	// sends bytes back to the client that contacted this server
	private void sendResponse(InetSocketAddress clientAddr, String response) {
		try {
			this.clientMessenger.send(clientAddr, response.getBytes(SingleServer.DEFAULT_ENCODING));
		} catch (IOException ignored) {
		}
	}

	// applier may work at diff speeds --> client thread waits <= 10 seconds for
	// lastApplied >= seq to account for slower applier
	private boolean waitUntilApplied(long seq, long maxMillis) {
		long deadline = System.currentTimeMillis() + maxMillis;
		while (System.currentTimeMillis() < deadline) {
			if (lastApplied.get() >= seq)
				return true;
			try {
				Thread.sleep(10);
			} catch (InterruptedException ie) {
				Thread.currentThread().interrupt();
				return false;
			}
		}
		return false;
	}

	private void truncateIfNeeded() {
		// skip truncation if we just recovered (give time to catch up)
		if (recentlyRecovered) {
			// After 100 applies we've caught up --> safe to truncate again
			if (appliedSinceTruncate.get() > 100) {
				recentlyRecovered = false;
				System.out.println(myID + " recovery complete, re-enabling truncation");
			} else {
				// don't truncate yet
				return; 
			}
		}

		// run only periodically
		if (appliedSinceTruncate.incrementAndGet() % TRUNCATE_EVERY_N_APPLIES != 0)
			return;

		try {
			// read the current zk log
			List<String> children = zk.getChildren(ZK_LOGS, false);
			// if the log alr small enough do nothing
			if (children.size() <= MAX_LOG_SIZE)
				return;
			// sort so we delete from oldest to newest
			children.sort(Comparator.comparingLong(MyDBFaultTolerantServerZK::parseSeq));
			// find  smallest log idx that all replicas have applied
			long min = Long.MAX_VALUE;
			for (String id : allReplicaIDs) {
				String path = ZK_PROGRESS + "/" + id;
				// if any replica hasnt reported progress yet dont truncate
				if (zk.exists(path, false) == null)
					return; // some replica hasn’t reported progress yet
				long v = Long.parseLong(new String(zk.getData(path, false, null), StandardCharsets.UTF_8));
				min = Math.min(min, v);
			}
			if (min == Long.MAX_VALUE)
				return;
			
			// add a safety margin so we dont delete entries that might still be needed
			long safeDeleteUpto = min - TRUNCATE_SAFETY_MARGIN;
			if (safeDeleteUpto < 0)
				return;

			// delete oldest entries while: (a) we're above MAX_LOG_SIZE and (b) safe to delete
			int idx = 0, deleteCount = 0;
			long firstDeleted = -1, lastDeleted = -1;

			while (children.size() - idx > MAX_LOG_SIZE && idx < children.size()) {
				String child = children.get(idx);
				long s = parseSeq(child);
				if (s > safeDeleteUpto)
					break;

				try {
					zk.delete(ZK_LOGS + "/" + child, -1);
					if (firstDeleted == -1)
						firstDeleted = s;
					lastDeleted = s;
					deleteCount++;
				} catch (KeeperException.NoNodeException ignored) {
					// another replica alr deleted it
				}
				idx++;
			}

			if (deleteCount > 0) {
				System.out.println(myID + " TRUNCATED " + deleteCount + " entries [" + firstDeleted + " to " + lastDeleted + "], " + "minApplied=" + min + ", safeDeleteUpto=" + safeDeleteUpto);
			}
			// record how far the log has been safely truncated
			writeZKMetadata(ZK_CHECKPOINT + "/seq", Long.toString(safeDeleteUpto).getBytes(StandardCharsets.UTF_8));

		} 
		catch (Exception ignored) {
			// failure here shouldnt crash the server
		}
	}

	// run once on startup after a crash
	// figures out where this server should resume applying the zk log from
	private void recoverFromZKLog() {
		try {
			// read the shared zk log
			List<String> children = zk.getChildren(ZK_LOGS, false);
			// log is empty --> nothing ever committed
			if (children.isEmpty()) {
				lastApplied.set(-1);
				System.out.println(myID + " recovering: log empty, starting from -1");
				return;
			}
			// sort to find the oldest entry still in the log
			children.sort(Comparator.comparingLong(MyDBFaultTolerantServerZK::parseSeq));
			long oldestSeq = parseSeq(children.get(0));
			long newestSeq = parseSeq(children.get(children.size() - 1));
			// start just before the oldest available entry --> apply loop will begin from oldestSeq
			lastApplied.set(oldestSeq - 1);
			System.out.println(myID + " recovering: log has " + children.size() + " entries [" + oldestSeq + " to " + newestSeq + "], " + "starting from seq=" + (oldestSeq - 1));

			// flag that we j recovered --> don't truncate for a while
			recentlyRecovered = true;

		} catch (Exception e) {
			System.err.println("Recovery failed for " + myID + ": " + e);
			lastApplied.set(-1);
		}
	}

	public static void main(String[] args) throws IOException {
		new MyDBFaultTolerantServerZK(
				NodeConfigUtils.getNodeConfigFromFile(args[0], ReplicatedServer.SERVER_PREFIX,
						ReplicatedServer.SERVER_PORT_OFFSET),
				args[1], args.length > 2 ? Util
						.getInetSocketAddressFromString(args[2]) : new InetSocketAddress("localhost", 9042));
	}
}
