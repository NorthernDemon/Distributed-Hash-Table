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

    private static int port;

    private static String host;

    static {
        try {
            Properties properties = new Properties();
            properties.load(new FileInputStream("service.properties"));
            port = Integer.parseInt(properties.getProperty("port"));
            host = properties.getProperty("host");
        } catch (IOException e) {
            logger.error(e);
        }
    }

    public static int getPort() {
        return port;
    }

    public static String getHost() {
        return host;
    }
}
