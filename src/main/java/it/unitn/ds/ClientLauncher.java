package it.unitn.ds;

import it.unitn.ds.server.NodeUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.rmi.Naming;

public final class ClientLauncher {

    private static final Logger logger = LogManager.getLogger();

    public static void main(String args[]) throws Exception {
        logger.info("Client started");
        try {
            NodeUtil remoteCal = (NodeUtil) Naming.lookup("rmi://localhost/CalendarImpl");
            long t1 = remoteCal.getNodes().getTime();
            long t2 = remoteCal.getNodes().getTime();
            logger.debug("This RMI call took " + (t2 - t1) + " milliseconds");
        } catch (Exception e) {
            logger.error(e);
        }
    }
}
