package DFS;

import UTILS.*;
import UTILS.Constants.*;

import java.net.*;
import java.util.*;

import java.io.Writer;
import java.io.FileWriter;
import java.io.IOException;
import java.io.EOFException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;

import java.nio.charset.StandardCharsets;
import java.nio.charset.Charset;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.nio.file.Files;

/*
  Object to be created by any client that wishes to interact with the DFS.
  Manages details of communication with the NameNode and DataNode(s).
 */
public class AccessPoint {

    static boolean verbose = false;

    static String IP_file = "NN_IP.txt";
    static String IP;
    static int port;
    
    static String NN_IP;
    static int NN_Port;

    private static Address my_address;
    private static Charset encoding = StandardCharsets.UTF_8;

    private ObjectOutputStream oos;
    private ObjectInputStream ois;
    private Socket sock = null;

    public AccessPoint(int p) {
	String NAMENODE_IP = read_NN_IP();
	NN_IP = NAMENODE_IP;
	NN_Port = UTILS.Constants.NAMENODE_PORT;

	try {
	    String IP = InetAddress.getLocalHost().getHostAddress();
	} catch (UnknownHostException e) {
	    e.printStackTrace();
	}
	Address a = new Address();
	a.set_IP(IP);
	a.set_port(p);
	my_address = a;
    }

    private static String read_NN_IP() {
	try {
	    String path = IP_file;
	    byte[] encoded = Files.readAllBytes(Paths.get(path));
	    return encoding.decode(ByteBuffer.wrap(encoded)).toString();
	} catch (IOException e) {
            e.printStackTrace();
	}
	return "";
    }

    /*
      private method to exchange messages with the NameNode.
     */
    private static Msg communicate(Msg msg)
    {
    	Socket sock;
    	Msg ret_msg = null;
	if (verbose) {
	    System.out.println(" [AP] > Contacting NameNode at IP " + NN_IP + " and port " + Integer.toString(NN_Port));
	}
	try {
	    sock = new Socket(NN_IP, NN_Port);
	    ObjectOutputStream oos = new ObjectOutputStream(sock.getOutputStream());
	    ObjectInputStream ois = new ObjectInputStream(sock.getInputStream());	    
	    oos.writeObject(msg);
	    ret_msg = (Msg) ois.readObject();	    
	    sock.close();
	} catch (UnknownHostException e1) {
	    e1.printStackTrace();
	} catch (IOException e1) {
	    System.out.println(" [AP] > Failed to connect to NameNode :(");
	} catch (ClassNotFoundException e) {
	    System.out.println(" [AP] > Did not receive reply from NameNode :(");
    	}	
	return ret_msg;
    }


    /*
      Lists all files available in DFS.
     */
    public ArrayList<String> ls() {
	ArrayList<String> result = null;
	Msg m = new Msg();
	m.set_msg_type(Constants.MESSAGE_TYPE.LS);
	m.set_return_address(my_address);
	Msg reply = communicate(m);
	if (reply != null) {
	    result = reply.get_arr_list();
	}
	return result;
    } 

    /*
      Lists all files available on current node.
      For use in MapReduce.
     */
    /*
    public ArrayList<String> ls_local() {
	// TODO
	} */

    /*
      Returns all data in target file (from some copy).
     */
    public String read_file(String filename) {
	// TODO
	return "";
    }

    /*
      Returns data with specified chunkname, if it is in the DFS.
     */
    public String read_chunk(ChunkName n) {
	Msg m = new Msg();
	m.set_msg_type(Constants.MESSAGE_TYPE.READ_CHUNK);
	m.set_chunk_name(n);
	Msg reply = communicate(m);
	if (reply != null && reply.get_msg_type() == Constants.MESSAGE_TYPE.READ_CHUNK_REPLY) {
	    if (verbose) {
		System.out.println(" [AP] > Received READ_CHUNK_REPLY from NameNode!");
	    }
	    return reply.get_data();
	} else {
	    if (verbose) {
		System.out.println(" [AP] > Did not get READ_CHUNK_REPLY from NameNode :(");
	    }
	    return null;
	}
    }

    /*
      Appends new data to target file (on all copies).
     */
    public void write_chunk(UTILS.Chunk c) {
	Msg m = new Msg();
	m.set_msg_type(Constants.MESSAGE_TYPE.WRITE);
	m.set_chunk_name(c.get_name());
	m.set_data(c.get_data());
	Msg reply = communicate(m);
	if (reply != null && reply.get_msg_type() == Constants.MESSAGE_TYPE.WRITE_REPLY) {
	    if (verbose) {
		System.out.println(" [AP] > Received WRITE_REPLY from NameNode!");
	    }
	} else {
	    if (verbose) {
		System.out.println(" [AP] > Did not get WRITE_REPLY from NameNode :(");
	    }
	}
    }

    public void greet_NN() throws InterruptedException, ClassNotFoundException, UnknownHostException {
	Msg greeting = new Msg();
	greeting.set_msg_type(Constants.MESSAGE_TYPE.CLIENT_GREETING);
	greeting.set_return_address(my_address);
	Msg reply = communicate(greeting);
	if (reply != null && reply.get_msg_type() == Constants.MESSAGE_TYPE.GREETING_REPLY) {
	    if (verbose) {
		System.out.println(" [AP] > Received GREETING_REPLY from NameNode!");
	    }
	} else {
	    if (verbose) {
		System.out.println(" [AP] > Did not get GREETING_REPLY from NameNode :(");
	    }
	}
    }

    public static void main(String args[])
    {

    }

}