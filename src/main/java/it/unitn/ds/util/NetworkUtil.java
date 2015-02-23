package it.unitn.ds.util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;

/**
 * Convenient class to work with networking
 */
public abstract class NetworkUtil {

    private static final Logger logger = LogManager.getLogger();

    private static final String LOCALHOST = "127.0.0.1";

    /**
     * List all possible IPv4 addresses of the current machine
     */
    public static void printMachineIPv4() {
        try {
            logger.info("Printing all possible IPv4 of your machine:");
            Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();
            while (networkInterfaces.hasMoreElements()) {
                Enumeration<InetAddress> inetAddresses = networkInterfaces.nextElement().getInetAddresses();
                while (inetAddresses.hasMoreElements()) {
                    String hostAddress = inetAddresses.nextElement().getHostAddress();
                    if (hostAddress.contains(".") && !hostAddress.equals(LOCALHOST)) {
                        logger.info(hostAddress);
                    }
                }
            }
        } catch (SocketException e) {
            logger.error("Failed to get network interface", e);
        }
    }
}
