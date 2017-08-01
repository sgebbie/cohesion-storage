/**
 * Cohesion Framework - Storage Library
 * Copyright (c) 2017 - Stewart Gebbie, Gethos. Licensed under the MIT licence.
 * vim: set ts=4 sw=0:
 */
package net.gethos.cohesion.common;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * @author {@literal Stewart Gebbie <sgebbie@gethos.net>}
 *
 */
public class Sha1Utils {

	public static final String SHA1_ALGORITHM = "SHA1";
	public static final Charset UTF8 = Charset.forName("UTF-8");

	public static byte[] sha1Path(String path) {
		return sha1Text(path);
	}

	public static byte[] sha1Text(String... text) {

		// calcuate SHA1 of the path
		try {
			MessageDigest sha1 = MessageDigest.getInstance(SHA1_ALGORITHM);

			if (text != null) {
				for (String t : text) {
					if (t != null) {
						ByteBuffer encoded = UTF8.encode(t);
						sha1.update(encoded);
					}
				}
			}
			byte[] digest = sha1.digest();

			return digest;
		} catch (NoSuchAlgorithmException e) {
			throw new RuntimeException("Unable to acquire digester",e);
		}
	}
}
