/**
 * Cohesion Framework - Storage Library
 * Copyright (c) 2017 - Stewart Gebbie, Gethos. Licensed under the MIT licence.
 * vim: set ts=4 sw=0:
 */
package net.gethos.cohesion.storage;

public class RuntimeIOException extends RuntimeException {
		private static final long serialVersionUID = 1L;
		
		public RuntimeIOException(String msg, Throwable cause) {
			super(msg,cause);
		}
		
		public RuntimeIOException(String msg) {
			super(msg);
		}
	}