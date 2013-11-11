package DFS;

import java.net.*;
import java.util.*;
import java.io.IOException;
import java.io.EOFException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import UTILS.*;
import UTILS.Constants.*;

public class NameNode {

    static String host;
    static int port;

    private static NameNode nn = null;
    private ServerSocket listener = null;

    // maps from the address of each available node to an array list of the file chunks it manages
    private HashMap<String, ArrayList<String>> available_nodes = new HashMap<String, ArrayList<String>>();
    // maps from each file chunk to the actual locations where it resides
    private HashMap<String, ArrayList<String>> data_locations = new HashMap<String, ArrayList<String>>();    

    private NameNode(int port) {
	/*
	try {
	    host = InetAddress.getLocalHost().getHostAddress();
	    System.out.println(" [NN] > Got NameNode host address: " + host);
	} catch (UnknownHostException e) {
	    System.out.println(" [NN] > Failed to get NameNode host address :(");
	    }*/
	host = UTILS.Constants.NAMENODE_IP;

	try {
	    listener = new ServerSocket(port);
	} catch (IOException e) {
	    System.out.println(" [NN] > Failed to start NameNode :(");
	}

    }

    public static NameNode getInstance(int port) {
	if (nn == null) {
	    nn = new NameNode(port);
	}
	return nn;
    }   

    /*
      When a new DataNode comes online, adds it to the available nodes.
     */
    private void add_node() {
	// TODO
    }

    /*
      Given a map from file_chunk IDs to memory-loaded data, 
      assigns each chunk to the least busy DataNodes that 
      don't already have it, according to the replication factor
      in the DFS config file. 

      Sends the data to each DataNode and awaits confirmation before
      updating available_nodes and data_locations, lest someone
      request the data right away. 
     */
    private void assign_chunks(HashMap<String, String> data) {
	// TODO
    }

    /*
      Removes failed_node from available_nodes. 
      Recopies the data failed_node was responsible for to other nodes.     
     */
    private void reassign_data(String failed_node) {
	// TODO
    }

    /* 
       Sends a message to each of the nodes in available_nodes. 
       If no reply is heard within some timeout period, the node is
       assumed dead, and its data is reassigned. 
     */
    private void poll() {
	// TODO
    }

    /*
      Returns a list of the available computing nodes.
     */
    /*
    public ArrayList<String> node_list() {
	// TODO
    }
    */

    /*
      Returns a list of all the files in the DFS.
     */
    /*
    public ArrayList<String> file_list() {
	// TODO
    }
    */

    /*
      Returns the actual location of a given file chunk,
      specified in a string formatted as filename_chunkID.
     */
    /*
    public String file_list(String filename_chunkID) {
	// TODO
    }
    */       

    /* 
       Listens for messages from DataNodes and clients.
       Periodically polls the nodes.
     */
    private void listen() {
	if (listener == null) {
	    System.out.println(" [NN] > Failed to establish socket :(");
	    return;
	}
	else {
	    while (true) {
		System.out.println(" [NN] > Listening for incoming messages... ");	
		try {
		    Socket sock = listener.accept();
		    ObjectInputStream ois = new ObjectInputStream(sock.getInputStream());
		    ObjectOutputStream oos = new ObjectOutputStream(sock.getOutputStream());
		    Msg msg = (Msg) ois.readObject();
		    System.out.println(" [NN] Received message!");
		    // TODO: 
		    // Msg ret_msg = this.process(msg);
		    // oos.writeObject(ret_msg);
		} catch (ClassNotFoundException e) {
		    e.printStackTrace();
		} catch (IOException e) {
		    System.out.println(" [NN] > Lost connection :(");
		}		
	    }	
	}
    }

    public static void main(String[] args) {

	System.out.println(" [NN] > Starting up new NameNode");	
	NameNode _nn = NameNode.getInstance(UTILS.Constants.NAMENODE_PORT);
	_nn.listen();

    }

}