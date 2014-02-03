package ca.sharvey.reddit.control;

import java.rmi.Remote;
import java.rmi.RemoteException;

import ca.sharvey.reddit.task.Result;
import ca.sharvey.reddit.task.Task;
import ca.sharvey.reddit.task.Type;

public interface Master extends Remote {
	public void registerHost(String host, int cores) throws RemoteException;
	public Task[] getTasks(String host, Type type, int num) throws RemoteException;
	public void updateResults(String host, Type type, Result[] result) throws RemoteException;
	public Task getTask(String host, Type type) throws RemoteException;
	public void updateResult(String host, Type type, Result results) throws RemoteException;
}
