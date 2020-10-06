package com.instaclustr.cassandra.ttl.cli;

public class TTLRemovalException extends Exception {

    public TTLRemovalException() {
    }

    public TTLRemovalException(String message) {
        super(message);
    }

    public TTLRemovalException(String message, Throwable cause) {
        super(message, cause);
    }
}
