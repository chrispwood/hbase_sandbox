// Based on HBase in Action by Nick Dimiduk & Amandeep Khurana
package org.wood.hbase.examples;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;

import utils.ShaUtils;

public class ActivitiesDao {

	public static final byte[] TABLE_NAME = Bytes.toBytes("twits");
	public static final byte[] TWITS_FAM = Bytes.toBytes("twits");

	public static final byte[] USERS_COL = Bytes.toBytes("user");
	public static final byte[] TWIT_COL = Bytes.toBytes("twit");

	private static final int LONG_LENGTH = 8;
  	private static final Logger log = Logger.getLogger(ActivitiesDao.class);

	private HTablePool pool;


	public ActivitiesDao(HTablePool pool) {
		this.pool = pool;
		createTable();
	}

	public void addTwit(String user, DateTime dt, String text) 
			throws IOException {
		HTableInterface twits = pool.getTable(TABLE_NAME);
		Put p = mkPut(new Twit(user, dt, text));
		twits.put(p);
		twits.close();
	}

	public List<org.wood.hbase.examples.Twit> getTwits(String user) throws IOException {
		HTableInterface twits = pool.getTable(TABLE_NAME);
		ResultScanner rscanner = twits.getScanner(mkScan(user));
		List<org.wood.hbase.examples.Twit> ret = 
			new ArrayList<org.wood.hbase.examples.Twit>();
		for(Result r : rscanner) {
			ret.add(new Twit(r));
		}

		twits.close();
		return ret;
	}

	public org.wood.hbase.examples.Twit getTwit(String user, DateTime dt) throws IOException {
		HTableInterface twits = pool.getTable(TABLE_NAME);
		Get g = mkGet(user, dt);

		Result result = twits.get(g);
		if(result.isEmpty()) {
			return null;
		}

		Twit t = new Twit(result);
		twits.close();
		return t;
	}

	private static Scan mkScan(String user) {
		byte[] userHash = ShaUtils.shasum(user);
    	byte[] startRow = Bytes.padTail(userHash, LONG_LENGTH); // 212d...866f00...
    	byte[] stopRow = Bytes.padTail(userHash, LONG_LENGTH);
    	stopRow[ShaUtils.SHA_LENGTH-1]++;                      // 212d...867000...

	    log.debug("Scan starting at: '" + convertToString(startRow) + "'");
	    log.debug("Scan stopping at: '" + convertToString(stopRow) + "'");

	    Scan s = new Scan(startRow, stopRow);
	    s.addColumn(TWITS_FAM, USERS_COL);
	    s.addColumn(TWITS_FAM, TWIT_COL);
	    return s;
	}

	private static Get mkGet(String user, DateTime dt) {
		Get g = new Get(mkRowKey(user, dt));
		g.addFamily(TWITS_FAM);
		return g;
	}

	private static byte[] mkRowKey(Twit twit) {
		return mkRowKey(twit.user, twit.dt);
	}

	private static byte[] mkRowKey(String user, DateTime dt) {
		byte[] userHash = ShaUtils.shasum(user);
		byte[] timestamp = Bytes.toBytes(-1 * dt.getMillis());
		byte[] rowkey = new byte[ShaUtils.SHA_LENGTH + LONG_LENGTH];

		int offset = 0;
    	offset = Bytes.putBytes(rowkey, offset, userHash, 0, userHash.length);
    	Bytes.putBytes(rowkey, offset, timestamp, 0, timestamp.length);
    	return rowkey;
	}

	private static Put mkPut(Twit twit) {
		Put p = new Put(mkRowKey(twit));
		p.add(TWITS_FAM, USERS_COL, Bytes.toBytes(twit.user));
		p.add(TWITS_FAM, TWIT_COL, Bytes.toBytes(twit.twit));
		return p;
	}

	private static void createTable() {
		Configuration conf = HBaseConfiguration.create();

		try {
			HBaseAdmin admin = new HBaseAdmin(conf);

			if(!admin.tableExists("twits")) {
				HTableDescriptor desc = new HTableDescriptor("twits");
				HColumnDescriptor c = new HColumnDescriptor("twits");
				c.setMaxVersions(1);
				desc.addFamily(c);
				admin.createTable(desc);
			}
		} catch(MasterNotRunningException e) {
			System.err.println("Error. HBase Master is not running.\n" + 
				e.getStackTrace());
		} catch(IOException ioe) {
			System.err.println("IO Error.\n" + 
				ioe.getStackTrace());
		}
	}

	private static String convertToString(byte[] bytes) {
		StringBuilder sb = new StringBuilder(bytes.length * 2);

		for(byte b : bytes) {
			sb.append(b).append(" ");
		}
		sb.deleteCharAt(sb.length() - 1);

		return sb.toString();
	}

	private static class Twit extends org.wood.hbase.examples.Twit {

		public Twit(Result result) {
			this(
				result.getColumnLatest(TWITS_FAM, USERS_COL).getValue(),
				Arrays.copyOfRange(result.getRow(), ShaUtils.SHA_LENGTH, ShaUtils.SHA_LENGTH + LONG_LENGTH),
				result.getColumnLatest(TWITS_FAM, TWIT_COL).getValue()
			);
		}

		public Twit(byte[] user, byte[] timestamp, byte[] twit) {
			this(
				Bytes.toString(user),
				new DateTime(-1 * Bytes.toLong(timestamp)),
				Bytes.toString(twit)
			);
		}

		public Twit(String user, DateTime timestamp, String twit) {
			this.user = user;
			this.dt = timestamp;
			this.twit = twit;
		}
	}
}
