package it.unitn.ds.util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.Scanner;
import java.util.regex.Pattern;

/**
 * Convenient class to work with infinite Scanner Input and invoking class methods with parameters
 */
public abstract class InputUtil {

    private static final Logger logger = LogManager.getLogger();

    private static final String SEPARATOR = ",";

    private static final Pattern INTEGER = Pattern.compile("^-?\\d+$");

    /**
     * Wait for user input indefinitely, which consist of method name and sequence of parameters, separated by SEPARATOR
     * <p>
     * Supported parameter types are:
     * - Integer
     * - String
     * <p>
     * Description: methodName,parameter1,parameter2
     * Example: sayLouderNTimes,5,I ate pillow
     *
     * @param className of the class to invoke public static methods in
     */
    public static void readInput(@NotNull String className) {
        Scanner scanner = new Scanner(System.in);
        while (scanner.hasNext()) {
            String[] commands = scanner.nextLine().split(SEPARATOR);
            Object[] params = new Object[commands.length - 1];
            Class<?>[] methodParameterTypes = new Class<?>[commands.length - 1];
            for (int i = 1; i < commands.length; i++) {
                int param = i - 1;
                if (INTEGER.matcher(commands[i]).find()) {
                    params[param] = Integer.parseInt(commands[i]);
                    methodParameterTypes[param] = int.class;
                } else {
                    params[param] = commands[i];
                    methodParameterTypes[param] = String.class;
                }
            }
            logger.debug("Calling method=" + commands[0] + Arrays.toString(params));
            try {
                Class.forName(className).getMethod(commands[0], methodParameterTypes).invoke(null, params);
            } catch (Exception e) {
                logger.error("Input scanner error", e);
            }
        }
    }
}
