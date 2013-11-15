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
    static int rep_factor = 2; // TODO: get from config file

    private static NameNode nn = null;
    private ServerSocket listener = null;

    // maps from the address of each available node to an array list of the file chunks it manages
    private HashMap<Address, ArrayList<ChunkName>> available_nodes = new HashMap<Address, ArrayList<ChunkName>>();
    // maps from the address of each available node to an ois connected to it
    private HashMap<Address, ObjectInputStream> ois_map = new HashMap<Address, ObjectInputStream>();
    // maps from the address of each available node to an oos connected to it
    private HashMap<Address, ObjectOutputStream> oos_map = new HashMap<Address, ObjectOutputStream>();
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
    private void add_node(Msg msg, ObjectInputStream ois, ObjectOutputStream oos) {
	Address a = msg.get_return_address();
	ArrayList<ChunkName> chunks = new ArrayList<ChunkName>();
	available_nodes.put(a, chunks);
	ois_map.put(a, ois);
	oos_map.put(a, oos);
    }

    private String get_chunk_data(ChunkName n) {
	String s = null;
	for (Map.Entry<Address, ArrayList<ChunkName>> entry : available_nodes.entrySet()) {
	    Address add = entry.getKey();
	    ArrayList<ChunkName> names = entry.getValue();
	    for (int i=0; i < names.size(); i++) {
		ChunkName cur = names.get(i);
		// If a DataNode has the desired chunk, 
		// get the data from it, then immediately return it
		if (cur == n) {
		    Msg m = new Msg();
		    m.set_msg_type(Constants.MESSAGE_TYPE.READ_MEM);
		    try {
			Msg reply = this.send_to_DN(m, add);
			return reply.get_data();
		    } catch (UnknownHostException e) {
			e.printStackTrace();
		    } catch (IOException e) {
			e.printStackTrace();
		    } catch (ClassNotFoundException e) {
			e.printStackTrace();
		    }	    
		}
	    }
	}
	return s;
    }

    private Msg send_to_DN(Msg m, Address a) throws UnknownHostException, IOException, ClassNotFoundException {
	System.out.println(" [NN] > Attempting to reach DataNode at IP " + a.get_IP() + " and port " + Integer.toString(a.get_port()));
	oos_map.get(a).writeObject(m);
	Msg ret_msg = (Msg) ois_map.get(a).readObject();	    
	return ret_msg;
    }

    /* 
       Returns ArrayList of DataNode addresses in order of increasing load.
     */
    private ArrayList<Address> sort_by_load(HashMap<Address, Integer> options) {
	ArrayList<Address> sorted_options = new ArrayList<Address>();
	while (options.size() > 0) {
	    Address min_add = null;
	    int min_load = -1;	    
	    for (Map.Entry<Address, Integer> entry : options.entrySet()) {
		Address add = entry.getKey();
		int load = entry.getValue();
		if (min_load == -1 || load < min_load) {
		    min_add = add;
		    min_load = load;
		}
	    }
	    sorted_options.add(min_add);
	    options.remove(min_add);
	}
	return sorted_options;
    }

    /*
      Assigns a chunk to the least busy DataNodes that 
      don't already have it, according to the replication factor
      in the DFS config file. 

      Sends the data to each DataNode and awaits confirmation before
      updating available_nodes and data_locations, lest someone
      request the data right away. 
     */
    private void assign_chunk(ChunkName n, String d) {

	System.out.println(" [NN] WARNING: assign_chunk() not fully implemented");
	int rep_count = 0;
	System.out.println(" [NN] >>> Number of available nodes: " + Integer.toString(available_nodes.size())); // temp

	// options lists DataNodes that don't already host this content. 
	// Maps from address to current load.
	HashMap<Address, Integer> options = new HashMap<Address, Integer>();
	for (Map.Entry<Address, ArrayList<ChunkName>> entry : available_nodes.entrySet()) {
	    Address add = entry.getKey();
	    ArrayList<ChunkName> load = entry.getValue();
	    // see if node contains chunk with this name
	    boolean found = false;
	    for (int i=0; i < load.size(); i++) {
		if (load.get(i) == n) {
		    found = true;
		}
	    }
	    // if node does not already contain this chunk: make it an option
	    if (!found) {
		options.put(add, load.size());
	    }
	}
	System.out.println(" [NN] >>> Number of options: " + Integer.toString(options.size())); // temp
	// sort options by current load. store chunk on least busy node(s)
	ArrayList<Address> sorted_options = this.sort_by_load(options);
	System.out.println(" [NN] >>> Number of sorted options: " + Integer.toString(sorted_options.size())); // temp
	for (int i=0; i < sorted_options.size(); i++) {
	    if (rep_count >= rep_factor) {
		break;
	    }
	    // try to send chunk to this address. if successful, increment rep_count.
	    Address a = sorted_options.get(i);
	    Msg m = new Msg();
	    m.set_msg_type(Constants.MESSAGE_TYPE.WRITE_MEM);
	    m.set_chunk_name(n);
	    m.set_data(d);
	    try {
		Msg reply = this.send_to_DN(m, a);
		rep_count += 1;
		// update metadata: this node now has this chunk
		ArrayList<ChunkName> names = available_nodes.get(a);
		names.add(n);
		available_nodes.put(a, names);		
		// update metadata: this chunk now stored at this location
		ArrayList<Address> locs = null;
		if (chunk_locations.containsKey(n)) {
		    locs = chunk_locations.get(n);
		} else {
		    locs = new ArrayList<Address>();
		}
		locs.add(a);
		chunk_locations.put(n, locs);
	    } catch (UnknownHostException e) {
		e.printStackTrace();
	    } catch (IOException e) {
		e.printStackTrace();
	    } catch (ClassNotFoundException e) {
		e.printStackTrace();
	    }	    
	}	    
	if (rep_count <	rep_factor) {
	    System.out.println(" [NN] WARNING: not enough DataNodes available to achieve desired replication factor");
	}

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
    private Msg process(Msg msg, ObjectInputStream ois, ObjectOutputStream oos) {
	Msg reply = new Msg();
	UTILS.Constants.MESSAGE_TYPE mt = msg.get_msg_type();
	if (mt == Constants.MESSAGE_TYPE.DATANODE_GREETING) {
	    System.out.println(" [NN] > Processing DATANODE_GREETING");
	    this.add_node(msg, ois, oos);
	    reply.set_msg_type(Constants.MESSAGE_TYPE.GREETING_REPLY);
	}
	if (mt == Constants.MESSAGE_TYPE.CLIENT_GREETING) {
	    System.out.println(" [NN] > Processing CLIENT_GREETING");
	    reply.set_msg_type(Constants.MESSAGE_TYPE.GREETING_REPLY);
	}
	if (mt == Constants.MESSAGE_TYPE.LS) {
	    System.out.println(" [NN] > Processing LS");
	    reply.set_msg_type(Constants.MESSAGE_TYPE.LS_REPLY);
	    ArrayList<String> l = this.file_list();
	    reply.set_arr_list(l);
	}
	if (mt == Constants.MESSAGE_TYPE.WRITE) {
	    System.out.println(" [NN] > Processing WRITE");
	    ChunkName n = msg.get_chunk_name();
	    String d = msg.get_data();
	    this.assign_chunk(n, d);
	    reply.set_msg_type(Constants.MESSAGE_TYPE.WRITE);
	}
	if (mt == Constants.MESSAGE_TYPE.READ_CHUNK) {
	    System.out.println(" [NN] > Processing READ_CHUNK");
	    ChunkName n = msg.get_chunk_name();
	    String d = this.get_chunk_data(n);
	    reply.set_msg_type(Constants.MESSAGE_TYPE.READ_CHUNK_REPLY);
	    reply.set_data(d);
	}
	// TODO: respond to other message types here
	return reply;
    }

    /* 
       Listens for messages from DataNodes and clients.
       Periodically polls the nodes. (TODO)
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
		    Msg reply = this.process(msg, ois, oos);
		    oos.writeObject(reply);
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