package org.wood.hbase.examples;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.client.HTablePool;
import org.joda.time.DateTime;

import org.wood.hbase.examples.ActivitiesDao;
import org.wood.hbase.examples.Twit;

public class ActivitiesTool {

	public static final String usage = 
		"ActivitiesTool action ...\n" +
		"  get user time - get the twit for the user at the given time.\n" +
		"  list user - get a list of all twits for the user.\n" +
		"  twit user message - post a twit by user.\n";

	public static void main(String[] args) {

		if(args.length==0 || args[0].toLowerCase().equals("help")) {
			System.out.println(usage);
			System.exit(0);
		}

		HTablePool pool = new HTablePool();
		ActivitiesDao dao = new ActivitiesDao(pool);

		if(args[0].toLowerCase().equals("get")) {
			if(args.length < 3) {
				System.out.println("Usage: get requires a user and time.\n" + usage);
				System.exit(1);
			}
			else {
				Twit twit = null;
				try {
					twit = dao.getTwit(args[1],DateTime.parse(args[2]));
				} catch (IOException ioe) {
					System.err.println(
						"Error reading from the HBase Twit table.\n"+
						ioe.getStackTrace());
					System.exit(2);
				}
				System.out.println(String.format("Twit <%s at %s: %s>",twit.user,twit.dt,twit.twit));
			}
		}
		else if(args[0].toLowerCase().equals("list")) {
			if(args.length < 2) {
				System.out.println("Usage: list requires a user name.\n" + usage);
				System.exit(1);
			}
			else {
				List<Twit> twits = null;
				try {
					twits = dao.getTwits(args[1]);
				} catch(IOException ioe) {
					System.err.println(
						"Error reading from the HBase Twit table.\n"+
						ioe.getStackTrace());
					System.exit(2);
				}
				System.out.println(String.format("Found %d twits from user %s:",twits.size(),args[1]));
				for(Twit twit : twits) {
					System.out.println(String.format("  Twit <%s at %s: %s>",twit.user,twit.dt,twit.twit));
				}
			}
		}
		else if(args[0].toLowerCase().equals("twit")) {

			if(args.length < 3) {
				System.out.println("Usage: twit requires a user name and message.\n" + usage);
				System.exit(1);
			}
			else {

				System.out.println("args check out");
				try {
					DateTime now = new DateTime();
					dao.addTwit(args[1],now,args[2]);
					Twit t = dao.getTwit(args[1],now);
					System.out.println(String.format("Successfully posted twit <%s at %s: %s>",
						args[1], now, args[2]));
				} catch(IOException ioe) {
					System.out.println(
						"Error writing to the HBase Twit table.\n"+
						ioe.getStackTrace());
					System.err.println(
						"Error writing to the HBase Twit table.\n"+
						ioe.getStackTrace());
					System.exit(2);
				}
			}
		}
		else {
			System.out.println(String.format("Usage: unknown command %s.\n%s", args[0], usage));
			System.exit(3);
		}
	}
}