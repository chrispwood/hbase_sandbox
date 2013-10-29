package utils;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.hadoop.hbase.util.Bytes;

public class ShaUtils {

	private static MessageDigest d;

	static {
		try {
			d = MessageDigest.getInstance("SHA");
		} catch(NoSuchAlgorithmException e) {
			throw new RuntimeException("Error. SHA does not exist.\n" + e.getStackTrace());
		}
	}

	public static final int SHA_LENGTH = 20;

	public static byte[] shasum(String s) {
		if(d==null) {
			throw new RuntimeException("Error. ShaUtils has not been initialized.");
		}
		else {
			return d.digest(Bytes.toBytes(s));
		}
	}
}