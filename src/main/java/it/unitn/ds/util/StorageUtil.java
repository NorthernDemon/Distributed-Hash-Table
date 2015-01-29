package it.unitn.ds.util;

import it.unitn.ds.Item;

public abstract class StorageUtil {

    public static boolean write(Item item) {
        // TODO write
        return true;
    }

    public Item read(int key) {
        return new Item(key, "some text in", 1);
    }
}
