package it.unitn.ds.rmi;

import it.unitn.ds.entity.Item;
import org.jetbrains.annotations.Nullable;

import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * Interface to be used by CLIENT
 */
public interface NodeClient extends Remote {

    @Nullable
    Item getItem(int key) throws RemoteException;

    @Nullable
    Item updateItem(int key, String value) throws RemoteException;
}
