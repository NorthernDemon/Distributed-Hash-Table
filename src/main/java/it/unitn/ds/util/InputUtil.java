package it.unitn.ds.util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

/**
 * Convenient class to work with Scanner for input and invoke methods with parameters
 */
public abstract class InputUtil {

    private static final Logger logger = LogManager.getLogger();

    /**
     * Wait for user input, which is comma separated list, consist of method name and array of parameters
     *
     * @param className of the class to invoke methods in
     */
    public static void readInput(String className) {
        try {
            Scanner scanner = new Scanner(System.in);
            while (scanner.hasNext()) {
                String[] commands = scanner.nextLine().split(",");
                Object[] params = new Object[commands.length - 1];
                List<Class<?>> methodParameterTypes = new ArrayList<>(commands.length);
                // TODO find a cleaner way to determine the parameter type
                for (int i = 1; i < commands.length; i++) {
                    try {
                        params[i - 1] = Integer.parseInt(commands[i]);
                        methodParameterTypes.add(int.class);
                    } catch (NumberFormatException eL) {
                        params[i - 1] = commands[i];
                        methodParameterTypes.add(String.class);
                    }
                    logger.trace("Parameter type: " + params[i - 1].getClass().getName());
                }
                logger.debug("Calling method with parameters: " + commands[0] + " | " + Arrays.toString(params));
                Class<?> aClass = Class.forName(className);
                Method method = aClass.getMethod(commands[0], convertListToArray(methodParameterTypes));
                method.invoke(null, params);
            }
        } catch (Exception e) {
            logger.error("Input scanner error", e);
            System.exit(1);
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
}
