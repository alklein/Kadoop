package DFS;

import UTILS.*;
import UTILS.Constants.*;

import java.net.*;
import java.util.*;
import java.io.IOException;
import java.io.EOFException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class DataNode {

    static String IP;
    static int port;

    private static DataNode dn = null;

    private ObjectOutputStream oos;
    private ObjectInputStream ois;
    private Socket sock = null;

    // list of data chunks this node is managing
    private ArrayList<String> my_files = new ArrayList<String>();

    private DataNode(int port) {
	try {
	    IP = InetAddress.getLocalHost().getHostAddress();
	    System.out.println(" [DN] > Got DataNode host address: " + IP);
	} catch (UnknownHostException e) {
	    System.out.println(" [DN] > Failed to get DataNode host address :(");
	}

	/*
	try {
	    listener = new ServerSocket(port);
	} catch (IOException e) {
	    System.out.println(" [DN] > Failed to start DataNode :(");
	    }*/

    }

    public static DataNode getInstance(int port) {
	if (dn == null) {
	    dn = new DataNode(port);
	}
	return dn;
    }   

    /*
      Stores a new chunk of data locally.
     */
    public void store(String filename_chunkID, String data) {
	// TODO
    }

    /*
      Writes to the end of an existing file.
      May be triggered by a client or e.g. a Reducer.
     */
    public void append(String filename_chunkID, String new_data) {
	// TODO
    }

    /*
      Reads and returns requested data. 
      Should be called *locally* by e.g. a Mapper or Reducer.
     */
    /*
    public String data(String filename_chunkID) {
	// TODO
    }
    */

    /*
      Receives and responds to messages from the NameNode.
     */
    public void listen_to_NN() throws IOException, ClassNotFoundException {
	System.out.println(" [DN] > Listening for messages");
	ois = new ObjectInputStream(sock.getInputStream());	
	while (true) { 
	    Msg msg = (Msg) ois.readObject();
	    System.out.println(" [DN] Received a message");
	    // TODO: parse msg  
	    // this.process(msg);  
	}
    }

    /*
      Sends message to NameNode.
     */
    public void write_to_NN(Msg m) throws IOException, ClassNotFoundException {
	oos.writeObject(m);
	oos.flush();
    }

    /*
      Announces its availability to the NameNode.
      Awaits further instruction.
     */
    public void connect() throws InterruptedException, ClassNotFoundException {
	try {
	    sock = new Socket(UTILS.Constants.NAMENODE_IP, UTILS.Constants.NAMENODE_PORT);
	    oos = new ObjectOutputStream(sock.getOutputStream());
	    Msg greeting = new Msg();
	    greeting.set_msg_type(Constants.MESSAGE_TYPE.DATANODE_GREETING);
	    this.write_to_NN(greeting);
	    this.listen_to_NN(); 
	} catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } 
    }
    

    public static void main(String[] args) {

	int hard_port = 10000; // TODO: get from config file
	System.out.println(" [DN] Starting up new DataNode");
	DataNode _dn = DataNode.getInstance(hard_port);
	try {
	    _dn.connect();
	} catch (InterruptedException e) {
	    e.printStackTrace();
	} catch (ClassNotFoundException e) {
	    e.printStackTrace();
	}
    }


}