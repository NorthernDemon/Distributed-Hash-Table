package it.unitn.ds.rmi;

import it.unitn.ds.entity.Item;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * Interface to be used by CLIENT for accessing the remote node via RMI
 */
public interface NodeClient extends Remote {

    @Nullable
    Item getItem(int key) throws RemoteException;

    @Nullable
    Item updateItem(int key, @NotNull String value) throws RemoteException;
}
