package it.unitn.ds;

import it.unitn.ds.server.NodeUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.rmi.Naming;

public final class ClientLauncher {

    private static final Logger logger = LogManager.getLogger();

    private static final String RMI_NODE = "rmi://localhost/NodeUtil";

    public static void main(String args[]) throws Exception {
        logger.info("Client started");
        try {
            NodeUtil remoteCal = (NodeUtil) Naming.lookup(RMI_NODE);
        } catch (Exception e) {
            logger.error("RMI error", e);
        }
    }
}
