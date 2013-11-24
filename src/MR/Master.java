/* 

Responsibilities:

track incoming job requests from API in queue
track available compute nodes
ping nodes (in separate thread); replace failed nodes
maintain queue of work
schedule + dispatch tasks to idle nodes
run each job in map phase then reduce phase 
     (complete all maps before dispatching reduces)

*/

package MR;

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

public class Master {

    static String IP_file = "Master_IP.txt";
    static String host;
    static int port;

    private static Master m = null;
    private ServerSocket listener = null;

    // maps from the address of each available node to an ois connected to it
    private HashMap<Address, ObjectInputStream> ois_map = new HashMap<Address, ObjectInputStream>();
    // maps from the address of each available node to an oos connected to it
    private HashMap<Address, ObjectOutputStream> oos_map = new HashMap<Address, ObjectOutputStream>();

    /*
      Writes Master's IP address to a file so ComputeNodes can find it.
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

    private Master(int port) {
	try {
	    host = InetAddress.getLocalHost().getHostAddress();
	} catch (UnknownHostException e) {
	    e.printStackTrace();	    
	}
	this.record_IP(host);
	System.out.println(" [MR] > Master started with host address: " + host);
	try {
	    listener = new ServerSocket(port);
	    System.out.println(" [MR] > Master started with port: " + Integer.toString(port));
	} catch (IOException e) {
	    System.out.println(" [MR] > Failed to start Master :(");
	    e.printStackTrace();	    
	}

    }

    public static Master getInstance(int port) {
	if (m == null) {
	    m = new Master(port);
	}
	return m;
    }   

    /*
      When a new ComputeNode comes online, adds it to the available nodes.
      Clobbers any existing record of a ComputeNode at that location.
     */
    private void add_node(Msg msg, ObjectInputStream ois, ObjectOutputStream oos) {
	Address a = msg.get_return_address();
	ois_map.put(a, ois);
	oos_map.put(a, oos);
    }


    private void assign(ArrayList<ChunkName> chunk_names, String class_name) {
	// TODO: assign map jobs to idle nodes
    }

    /*
      Parses and processes incoming messages.
     */
    private Msg process(Msg msg, ObjectInputStream ois, ObjectOutputStream oos) {
	Msg reply = new Msg();
	UTILS.Constants.MESSAGE_TYPE mt = msg.get_msg_type();
	if (mt == Constants.MESSAGE_TYPE.COMPUTENODE_GREETING) {
	    System.out.println(" [MR] > Processing COMPUTENODE_GREETING");
	    this.add_node(msg, ois, oos);
	    reply.set_msg_type(Constants.MESSAGE_TYPE.GREETING_REPLY);
	}
	if (mt == Constants.MESSAGE_TYPE.ASSIGN_MAPS) {
	    System.out.println(" [MR] > Processing ASSIGN_MAPS");
	    String class_name = msg.get_class_name();
	    ArrayList<ChunkName> chunk_names = msg.get_chunk_names();
	    this.assign(chunk_names, class_name);
	    reply.set_msg_type(Constants.MESSAGE_TYPE.ASSIGN_MAPS_REPLY);
	}
	// TODO: respond to other message types here
	return reply;
    }

    /* 
       Listens for messages from ComputeNodes and clients.
       Periodically polls the nodes. (TODO)
     */
    private void listen() {
	if (listener == null) {
	    System.out.println(" [MR] > Failed to establish socket :(");
	    return;
	}
	else {
	    while (true) {
		System.out.println(" [MR] > Listening for incoming messages... ");	
		try {
		    Socket sock = listener.accept();
		    ObjectInputStream ois = new ObjectInputStream(sock.getInputStream());
		    ObjectOutputStream oos = new ObjectOutputStream(sock.getOutputStream());
		    Msg msg = (Msg) ois.readObject();
		    System.out.println(" [MR] > Received message!");
		    Msg reply = this.process(msg, ois, oos);
		    oos.writeObject(reply);
		} catch (ClassNotFoundException e) {
		    e.printStackTrace();
		} catch (IOException e) {
		    System.out.println(" [MR] > Lost connection :(");
		}		
	    }	
	}
    }


    public static void main(String[] args) {
	System.out.println(" [MR] > Starting up Master node");	
        Master _m = Master.getInstance(UTILS.Constants.MASTER_PORT);
	_m.listen();
    }


}