package server.faulttolerance;

import com.datastax.driver.core.Cluster;
import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.paxospackets.RequestPacket;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import com.datastax.driver.core.Session;

/**
 * This class should implement your {@link Replicable} database app if you wish
 * to use Gigapaxos.
 * <p>
 * Make sure that both a single instance of Cassandra is running at the default
 * port on localhost before testing.
 * <p>
 * Tips:
 * <p>
 * 1) No server-server communication is permitted or necessary as you are using
 * gigapaxos for all that.
 * <p>
 * 2) A {@link Replicable} must be agnostic to "myID" as it is a standalone
 * replication-agnostic application that via its {@link Replicable} interface is
 * being replicated by gigapaxos. However, in this assignment, we need myID as
 * each replica uses a different keyspace (because we are pretending different
 * replicas are like different keyspaces), so we use myID only for initiating
 * the connection to the backend data store.
 * <p>
 * 3) This class is never instantiated via a main method. You can have a main
 * method for your own testing purposes but it won't be invoked by any of
 * Grader's tests.
 */
public class MyDBReplicableAppGP implements Replicable {

	/**
	 * Set this value to as small a value with which you can get tests to still
	 * pass. The lower it is, the faster your implementation is. Grader* will
	 * use this value provided it is no greater than its MAX_SLEEP limit.
	 * Faster
	 * is not necessarily better, so don't sweat speed. Focus on safety.
	 */
	public static final int SLEEP = 1000;

	// the only table Grader uses for application state
	private static final String TABLE_NAME = "grade";

	private final Cluster cluster;
	private final Session session;

	/**
	 * All Gigapaxos apps must either support a no-args constructor or a
	 * constructor taking a String[] as the only argument. Gigapaxos relies on
	 * adherence to this policy in order to be able to reflectively construct
	 * customer application instances.
	 *
	 * @param args Singleton array whose args[0] specifies the keyspace in the
	 *             backend data store to which this server must connect.
	 *             Optional args[1] and args[2]
	 * @throws IOException
	 */
	public MyDBReplicableAppGP(String[] args) throws IOException {
		String keyspace = args[0];
		cluster = Cluster.builder().addContactPoint("127.0.0.1").withPort(9042).build();
		session = cluster.connect(keyspace);
	}

	@Override
	public boolean execute(Request request, boolean b) {
		return execute(request);
	}

	@Override
	public boolean execute(Request request) {
		try {
			RequestPacket rp = (RequestPacket) request;
			String requestValue = rp.requestValue;

			if (requestValue == null) {
				return true;
			}

			// requestValue is just the raw cql query string
			String result = executeCQLQuery(requestValue);
			rp.setResponse(result);
			return true;

		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}

	@Override
	public String checkpoint(String s) {
		// reads entire table, converts to json, concatenates rows w/ new line char, returns tha snapshot
		// eg turns table into
		// {"ssn":1,"firstname":"Ipsita"}
		// {"ssn":2,"firstname":"Sunny"}
		// ... more json rows sep by /n
		// different from col=value,col=value stuff
		try {
			String out = "";

			var resultSet = session.execute("SELECT JSON * FROM " + TABLE_NAME);
			for (var row : resultSet) {
				out = out + row.getString(0) + "\n";
			}
			return out;

		} catch (Exception e) {
			e.printStackTrace();
			return "";
		}
	}

	@Override
	public boolean restore(String s, String s1) {
		// takes a snapshot and rebuilds the cassandra table
		// different from col=value,col=value stuff
		try {
			// wiping current table contents
			session.execute("TRUNCATE " + TABLE_NAME);

			// no checkpoint
			if (s1 == null || s1.trim().isEmpty()) {
				return true;
			}

			// checkpoint exists
			// split on any line break, eg in our case the "\n" would trigger it; assumes that we have one json obj per line which we did in cehckpoint
			String[] rows = s1.split("\\R");

			for (String row : rows) {
				String json = row.trim();
				// insert each row back into cassandra
				session.execute("INSERT INTO " + TABLE_NAME + " JSON '" + json + "'");
			}
			return true;

		} catch (Exception e) {
			e.printStackTrace();
			return true;
		}
	}

	@Override
	public Request getRequest(String s) throws RequestParseException {
		return null;
	}

	@Override
	public Set<IntegerPacketType> getRequestTypes() {
		return new HashSet<IntegerPacketType>();
	}

	private String executeCQLQuery(String cqlQuery) {
		try {
			// executing the query gives us table like the following:
			// https://www.tutorialspoint.com/cassandra/cassandra_read_data.htm
			var resultSet = session.execute(cqlQuery);

			// need to turn tha table, comprised of rows and cols, into string input for
			// client to parse and copare results across replicas
			var columnDefinitions = resultSet.getColumnDefinitions();

			// INSERT/UPDATE queries don't return cols, need to account for that
			if (columnDefinitions == null || columnDefinitions.size() == 0) {
				return "OK";
			}

			// the following logic would convert to smt like
			// "ssn=1,firstname=Ipsita;ssn=2,firstname=Sunny;" --> picked this bc easy to parse
			String resultString = "";

			// iterate over each returned row
			for (var row : resultSet) {
				// iterate over each col in the row
				for (int colIndex = 0; colIndex < columnDefinitions.size(); colIndex++) {
					// separate col entries w/ commas
					if (colIndex > 0) {
						resultString += ",";
					}
					String columnName = columnDefinitions.getName(colIndex);
					Object columnValue = row.isNull(colIndex) ? null : row.getObject(colIndex);

					// represent column as name=value
					if (columnValue == null) {
						resultString += columnName + "=null";
					} else {
						resultString += columnName + "=" + columnValue.toString();
					}
				}

				// separate rows with semicolons
				resultString += ";";
			}
			return resultString;

		} catch (Exception e) {
			return "ERROR:" + e.getMessage();
		}
	}
}
