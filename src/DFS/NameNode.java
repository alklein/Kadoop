package DFS;

import UTILS.*;
import UTILS.Constants.*;

import java.net.*;
import java.util.*;
import java.io.Writer;
import java.io.FileWriter;
import java.io.IOException;
import java.io.EOFException;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class NameNode {

    static String IP_file = "NN_IP.txt";
    static String host;
    static int port;

    private static NameNode nn = null;
    private ServerSocket listener = null;

    // maps from the address of each available node to an array list of the file chunks it manages
    private HashMap<Address, ArrayList<String>> available_nodes = new HashMap<Address, ArrayList<String>>();
    // maps from each filename to the chunks that comprise it
    private HashMap<String, ArrayList<String>> chunk_IDs = new HashMap<String, ArrayList<String>>();
    // maps from each file chunk to the actual locations where it resides
    private HashMap<ChunkName, ArrayList<Address>> chunk_locations = new HashMap<ChunkName, ArrayList<Address>>();    

    /*
      Writes NameNode's IP address to a file so DataNodes can find it.
     */
    private void record_IP(String ip) {
	try {
	    String path = this.IP_file;
	    Writer output;
	    output = new BufferedWriter(new FileWriter(path, false));
	    output.write(ip);
	    output.close(); 
	} catch (IOException e) {
	    e.printStackTrace();	    
	}
    }

    private NameNode(int port) {
	try {
	    host = InetAddress.getLocalHost().getHostAddress();
	} catch (UnknownHostException e) {
	    e.printStackTrace();	    
	}
	this.record_IP(host);
	System.out.println(" [NN] > NameNode started with host address: " + host);
	try {
	    listener = new ServerSocket(port);
	    System.out.println(" [NN] > NameNode started with port: " + Integer.toString(port));
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
      Clobbers any existing record of a DataNode at that location.
     */
    private void add_node(Msg msg) {
	Address a = msg.get_return_address();
	ArrayList<String> chunks = new ArrayList<String>();
	available_nodes.put(a, chunks);
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
      Returns a list of the available computing nodes by address.
     */
    public ArrayList<Address> node_list() {
	ArrayList<Address> a = new ArrayList<Address>();
	for (Address key : available_nodes.keySet()) {
	    a.add(key);
	}
	return a;
    }

    /*
      Returns a list of all the files in the DFS.
     */
    public ArrayList<String> file_list() {
	ArrayList<String> a = new ArrayList<String>();
	for (String key : chunk_IDs.keySet()) {
	    a.add(key);
	}
	return a;
    }

    /*
      Returns the actual locations of a given file chunk.
      If the chunk is not in the DFS, returns null.
     */
    public ArrayList<Address> where_is(ChunkName name) {
	if (chunk_locations.containsKey(name)) {
	    return chunk_locations.get(name);
	}
	else {
	    return null;
	}
    }

    /*
      Parses and processes incoming messages.
     */
    private void process(Msg msg) {
	UTILS.Constants.MESSAGE_TYPE mt = msg.get_msg_type();
	if (mt == Constants.MESSAGE_TYPE.DATANODE_GREETING) {
	    System.out.println(" [NN] > Processing DATANODE_GREETING");
	    this.add_node(msg);
	}
	if (mt == Constants.MESSAGE_TYPE.CLIENT_GREETING) {
	    System.out.println(" [NN] > Processing CLIENT_GREETING");
	    this.add_node(msg);
	}
	// TODO: respond to other message types here
    }

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
		    System.out.println(" [NN] > Received message!");
		    this.process(msg);
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