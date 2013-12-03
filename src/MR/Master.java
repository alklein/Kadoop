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

    private void remove_node(Address a) {
	ois_map.remove(a);
	oos_map.remove(a);
    }

    /*
      Sends message to a ComputeNode.
     */
    private Msg send_to_CN(Msg m, Address a) throws UnknownHostException, IOException, ClassNotFoundException {
	System.out.println(" [MR] > Attempting to reach ComputeNode at IP " + a.get_IP() + " and port " + Integer.toString(a.get_port()));
	oos_map.get(a).writeObject(m);
	oos_map.get(a).flush();
	Msg ret_msg = (Msg) ois_map.get(a).readObject();	    
	return ret_msg;
    }

    /*
      Naive assignment of tasks to ComputeNodes. 
      TODO: parallelize; exploit locality in disc mode. 
     */
    private ArrayList<ChunkName> assign_map(ArrayList<ChunkName> chunk_names, String class_name) {
	System.out.println(" [MR] Trying to assign this many chunk maps: " + Integer.toString(chunk_names.size())); 
	ArrayList<ChunkName> mapped_chunk_names = new ArrayList<ChunkName>();
	long timeoutExpiredMs = System.currentTimeMillis() + UTILS.Constants.MAP_TIMEOUT_TOTAL;	
	while (chunk_names.size() > 0) {
	    // check timeout condition	  
	    if (System.currentTimeMillis() >= timeoutExpiredMs) {
		    System.out.println(" [MR] Map computation timed out before completion :(");
		    break;
	    }  
	    for (Address add : ois_map.keySet()) {
		// check timeout condition
		if (System.currentTimeMillis() >= timeoutExpiredMs) {
		    break;
		}

		// try to dispatch task to that node
		ChunkName cur_chunk = chunk_names.get(0);
		Msg m = new Msg();
		m.set_msg_type(Constants.MESSAGE_TYPE.MAP);
		m.set_chunk_name(cur_chunk);
		m.set_class_name(class_name);
		Timer timer = new Timer(true);
		//InterruptTimerTask interruptTimerTask = new InterruptTimerTask(Thread.currentThread());
		IRR interruptTimerTask = new IRR(Thread.currentThread());
		timer.schedule(interruptTimerTask, UTILS.Constants.MAP_TIMEOUT_NODE);
		try {		    
		    Msg reply = this.send_to_CN(m, add);
		    // if success: record name of mapped chunk;
		    // remove current chunk from chunk_names
		    System.out.println(" [MR] Map completed by ComputeNode. Name of mapped file: " + reply.get_chunk_name().to_String());
		    mapped_chunk_names.add(reply.get_chunk_name());
		    chunk_names.remove(0);  
		// else: mark node as unavailable
		} catch (UnknownHostException e) {
		    e.printStackTrace();
		    this.remove_node(add);
		} catch (IOException e) {
		    e.printStackTrace();
		    this.remove_node(add);
		} catch (ClassNotFoundException e) {
		    e.printStackTrace();
		    this.remove_node(add);
		} finally {
		    timer.cancel();
		}	    
	    }

	  /*
	  Logic:
	  while chunk_names is non-empty and time < timeout:
	  assign work to initial list of available nodes.
	  then listen to computenodes. whenever one requests work, 
	  mark its last work as done (remove it from chunk_names).
	  then send it a new task. 

	  Use blocking queue?

	 */   
	}
	if (chunk_names.size() == 0) {
	    System.out.println(" [MR] Map successful!");
	    return mapped_chunk_names; // work completed
	} else { 
	    System.out.println(" [MR] Map unsuccessful: tasks not fully completed :(");
	    return null; // computation timed out
	}
    }

    private ChunkName assign_reduce(ArrayList<ChunkName> mapped_chunk_names, String class_name) {
	System.out.println(" [MR] Trying to assign this many chunk reduces: " + Integer.toString(mapped_chunk_names.size())); 
	ChunkName reduced_chunk_name = null;
	long timeoutExpiredMs = System.currentTimeMillis() + UTILS.Constants.MAP_TIMEOUT_TOTAL;	// use same timeout as map phase
	while (mapped_chunk_names.size() > 0) {
	    // check timeout condition	  
	    if (System.currentTimeMillis() >= timeoutExpiredMs) {
		    System.out.println(" [MR] Reduce computation timed out before completion :(");
		    break;
	    }  
	    for (Address add : ois_map.keySet()) {
		// check timeout condition
		if (System.currentTimeMillis() >= timeoutExpiredMs) {
		    break;
		}

		// try to dispatch entire task to first node
		Msg m = new Msg();
		m.set_msg_type(Constants.MESSAGE_TYPE.REDUCE);
		m.set_chunk_names(mapped_chunk_names);
		m.set_class_name(class_name);
		Timer timer = new Timer(true);
		//InterruptTimerTask interruptTimerTask = new InterruptTimerTask(Thread.currentThread());
		IRR interruptTimerTask = new IRR(Thread.currentThread());
		timer.schedule(interruptTimerTask, UTILS.Constants.MAP_TIMEOUT_NODE);
		try {		    
		    Msg reply = this.send_to_CN(m, add);
		    // if success: record name of mapped chunk;
		    // remove current chunk from chunk_names
		    if (reply.get_chunk_name() != null) {
			System.out.println(" [MR] Reduce completed by ComputeNode. Name of reduced file: " + reply.get_chunk_name().to_String());
		    }
		    reduced_chunk_name = reply.get_chunk_name();
		    mapped_chunk_names = new ArrayList<ChunkName>(); // empty
		    break;
		// else: mark node as unavailable
		} catch (UnknownHostException e) {
		    e.printStackTrace();
		    this.remove_node(add);
		} catch (IOException e) {
		    e.printStackTrace();
		    this.remove_node(add);
		} catch (ClassNotFoundException e) {
		    e.printStackTrace();
		    this.remove_node(add);
		} finally {
		    timer.cancel();
		}	    
	    }
	}
	if (mapped_chunk_names.size() == 0) {
	    System.out.println(" [MR] Reduce successful!");
	    return reduced_chunk_name; // work completed
	} else { 
	    System.out.println(" [MR] Reduce unsuccessful: tasks not fully completed :(");
	    return null; // computation timed out
	}

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
	if (mt == Constants.MESSAGE_TYPE.KADOOP_GREETING) {
	    System.out.println(" [MR] > Processing KADOOP_GREETING");
	    reply.set_msg_type(Constants.MESSAGE_TYPE.GREETING_REPLY);
	}
	if (mt == Constants.MESSAGE_TYPE.ASSIGN_MAPS) {
	    System.out.println(" [MR] > Processing ASSIGN_MAPS");
	    String class_name = msg.get_class_name();
	    ArrayList<ChunkName> chunk_names = msg.get_chunk_names();
	    ArrayList<ChunkName> mapped_chunk_names = this.assign_map(chunk_names, class_name);
	    boolean success = (!(mapped_chunk_names == null));
	    reply.set_success(success);
	    reply.set_chunk_names(mapped_chunk_names);
	    reply.set_msg_type(Constants.MESSAGE_TYPE.ASSIGN_MAPS_REPLY);
	}
	if (mt == Constants.MESSAGE_TYPE.ASSIGN_REDUCES) {
	    System.out.println(" [MR] > Processing ASSIGN_REDUCES");
	    String class_name = msg.get_class_name();
	    ArrayList<ChunkName> mapped_chunk_names = msg.get_chunk_names();
	    ChunkName reduced_chunk_name = this.assign_reduce(mapped_chunk_names, class_name);
	    boolean success = (!(reduced_chunk_name == null));
	    reply.set_success(success);
	    reply.set_chunk_name(reduced_chunk_name);
	    reply.set_msg_type(Constants.MESSAGE_TYPE.ASSIGN_REDUCES_REPLY);
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
		    oos.flush();
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