package it.unitn.ds;

import it.unitn.ds.entity.ReplicationTimeout;
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

    public static final String CONFIGURATION_FILE = "service.properties";

    private static int rmiPort;

    private static ReplicationTimeout replicationTimeout;

    private static int replicationW;

    private static int replicationR;

    private static int replicationN;

    static {
        try {
            Properties properties = new Properties();
            properties.load(new FileInputStream(CONFIGURATION_FILE));
            rmiPort = Integer.parseInt(properties.getProperty("rmi-port"));
            int replicationTimeoutValue = Integer.parseInt(properties.getProperty("replication-timeout-value"));
            String replicationTimeoutUnit = properties.getProperty("replication-timeout-unit");
            replicationTimeout = new ReplicationTimeout(replicationTimeoutValue, replicationTimeoutUnit);
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

    public static ReplicationTimeout getReplicationTimeout() {
        return replicationTimeout;
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
