package it.unitn.ds.util;

import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import com.google.common.io.Files;
import it.unitn.ds.server.Item;
import it.unitn.ds.server.Node;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.util.*;

public abstract class StorageUtil {

    private static final Logger logger = LogManager.getLogger();

    public static void main(String[] args) {
        Node node = new Node(1);
        Map<Integer, Item> i = new HashMap<>();
        i.put(11, new Item(11, "I am a Cow11", 1));
        i.put(12, new Item(12, "I am a Cow12", 2));
        i.put(13, new Item(13, "I am a Cow13", 3));
        node.setItems(i);
        write(node, new Item(10, "I am a Cow", 1));
        write(node, new Item(13, "I am a Cow13 updated", 4));
        logger.info(Arrays.toString(node.getItems().values().toArray()));
        logger.info(read(node, 11));
        logger.info(read(node, 12));
        logger.info(read(node, 13));
        logger.info(read(node, 10));
        logger.info(read(node, 9));
    }

    public static void write(Node node, Item newItem) {
        try (PrintWriter writer = new PrintWriter(getFileName(node), "UTF-8")) {
            Map<Integer, Item> items = node.getItems();
            items.put(newItem.getKey(), newItem);
            for (Item item : items.values()) {
                writer.write(item.getKey() + "," + item.getValue() + "," + item.getVersion() + "\n");
            }
        } catch (FileNotFoundException | UnsupportedEncodingException e) {
            logger.error(e);
        }
    }

    public static Item read(Node node, int key) {
        try {
            List<String> lines = Files.readLines(new File(getFileName(node)), Charsets.UTF_8);
            for (String line : lines) {
                if (line.startsWith(key + ",")) {
                    Iterator<String> it = Splitter.on(",").split(line).iterator();
                    return new Item(Integer.parseInt(it.next()), it.next(), Integer.parseInt(it.next()));
                }
            }
        } catch (IOException e) {
            logger.error(e);
        }
        return null;
    }

    private static String getFileName(Node node) {
        return "Node-" + node.getId() + ".txt";
    }
}
