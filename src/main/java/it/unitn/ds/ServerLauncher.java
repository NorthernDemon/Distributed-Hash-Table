package it.unitn.ds;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public final class ServerLauncher {

    private static final Logger logger = LogManager.getLogger();

    public static void main(String[] args) throws Exception {
        logger.info("Server started");
    }
}
