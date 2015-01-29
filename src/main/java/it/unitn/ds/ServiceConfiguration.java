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

    public final static String SCHEMA = "lifestylecoach";

    public final static String NAME = '/' + SCHEMA;

    private static int port;

    private static String host;

    private static String url;

    private static String wsdl;

    static {
        try {
            Properties properties = new Properties();
            properties.load(new FileInputStream("service.properties"));
            port = Integer.parseInt(properties.getProperty("port"));
            host = properties.getProperty("host");
            url = "http://" + host + ":" + port + NAME;
            wsdl = "http://" + host + ":" + Integer.parseInt(properties.getProperty("wsdl-port")) + NAME + "?wsdl";
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

    public static String getName() {
        return NAME;
    }

    public static String getUrl() {
        return url;
    }

    public static String getWsdl() {
        return wsdl;
    }
}
