package it.unitn.ds;

import it.unitn.ds.util.InputUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public final class ClientLauncher {

    private static final Logger logger = LogManager.getLogger();

    /**
     * ./client.jar [{methodName},{operation GET|UPDATE},{Node ID},{key},{value - OPTIONAL}]
     * <p/>
     * Example: [get,10,12]
     * Example: [update,15,12,New Value Item]
     *
     * @param args
     */
    public static void main(String args[]) throws Exception {
        logger.info("Client is ready for request>>");
        logger.info("Example: [{methodName},{operation GET|UPDATE},{Node ID},{key},{value - OPTIONAL}]");
        logger.info("Example: [get,10,12]");
        logger.info("Example: [update,15,12,New Value Item]");
        InputUtil.readInput("it.unitn.ds.ClientLauncher");
    }

    public static void get(int nodeId, int key) {
        logger.info("Get from nodeId=" + nodeId + ", key=" + key);
    }

    public static void update(int nodeId, int key, String value) {
        logger.info("Update nodeId=" + nodeId + ", key=" + key + ", update=" + value);
        if (value.contains(",")) {
            logger.warn("Cannot store commas in value field... yet!");
            return;
        }
    }
}
