package it.unitn.ds;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * Reads property file with service configuration
 */
public abstract class ServiceConfiguration {

    private static final Logger logger = LogManager.getLogger();

    private static int rmiPort;

    private static int replicationW;

    private static int replicationR;

    private static int replicationN;

    static {
        try {
            Properties properties = new Properties();
            properties.load(new FileInputStream("service.properties"));
            rmiPort = Integer.parseInt(properties.getProperty("rmi-port"));
            replicationW = Integer.parseInt(properties.getProperty("replication-w"));
            replicationR = Integer.parseInt(properties.getProperty("replication-r"));
            replicationN = Integer.parseInt(properties.getProperty("replication-n"));
        } catch (IOException e) {
            logger.error("Failed to load service configuration!", e);
        }
    }

    public static int getRmiPort() {
        return rmiPort;
    }

    public static int getReplicationW() {
        return replicationW;
    }

    public static int getReplicationR() {
        return replicationR;
    }

    public static int getReplicationN() {
        return replicationN;
    }
}
