package it.unitn.ds;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

public final class ClientLauncher {

    private static final Logger logger = LogManager.getLogger();

    private static final String RMI_NODE = "rmi://localhost/NodeUtil";

    /**
     * ./client.jar {operation GET|UPDATE},{Node ID},{key},{value - OPTIONAL}
     * <p/>
     * Example: [1099,10,0]
     * Example: [1100,15,10]
     *
     * @param args
     */
    public static void main(String args[]) throws Exception {
        logger.info("Client is ready for request>>");
        logger.info("Example: [{operation GET|UPDATE},{Node ID},{key},{value - OPTIONAL}]");
        logger.info("Example: [get,10,12]");
        logger.info("Example: [update,15,12,New Value Item]");
        Scanner scanner = new Scanner(System.in);
        while (scanner.hasNext()) {
            String[] commands = scanner.nextLine().split(",");
            Object[] params = new Object[commands.length - 1];
            List<Class<?>> methodParameterTypes = new ArrayList<>();
            // TODO find a cleaner way to determine the parameter type
            for (int i = 1; i < commands.length; i++) {
                try {
                    params[i - 1] = Integer.parseInt(commands[i]);
                    methodParameterTypes.add(int.class);
                } catch (NumberFormatException eL) {
                    params[i - 1] = commands[i];
                    methodParameterTypes.add(String.class);
                }
                System.out.println("Param Type | " + params[i - 1].getClass().getName());
            }
            logger.debug("Calling | " + commands[0] + " | " + Arrays.toString(params));
            Class<?> aClass = Class.forName("it.unitn.ds.ClientLauncher");
            Method method = aClass.getMethod(commands[0], convertListToArray(methodParameterTypes));
            method.invoke(null, params);
        }
    }

    /**
     * Helper method to convert List of classes with unknown type into array
     *
     * @param methodParameterTypes list of class types
     * @return array of class types
     */
    private static Class<?>[] convertListToArray(List<Class<?>> methodParameterTypes) {
        Class<?>[] array = new Class<?>[methodParameterTypes.size()];
        int i = 0;
        for (Class<?> parameterType : methodParameterTypes) {
            array[i++] = parameterType;
        }
        return array;
    }

    public static void get(int nodeId, int key) {
        logger.info("Get from nodeId=" + nodeId + ", key=" + key);
    }

    public static void update(int nodeId, int key, String value) {
        logger.info("Update nodeId=" + nodeId + ", key=" + key + ", update=" + value);
    }
}
