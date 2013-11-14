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

    static String IP_file = "NN_IP.txt";
    static String IP;
    static int port;
    
    static String M_IP;
    static int M_Port;

    private static Address my_address;
    private static Charset encoding = StandardCharsets.UTF_8;

    private ObjectOutputStream oos;
    private ObjectInputStream ois;
    private Socket sock = null;

    public AccessPoint(int p) {
	String NAMENODE_IP = read_NN_IP();
	M_IP = NAMENODE_IP;
	M_Port = UTILS.Constants.NAMENODE_PORT;

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
      Sends message to NameNode.
     */
    private void write_to_NN(Msg m) throws IOException, ClassNotFoundException {
	oos.writeObject(m);
	oos.flush();
    }


    /*
      Lists all files available in DFS.
     */
    /*
    public ArrayList<String> ls() {
	// TODO
	} */

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
    /*
    public String read(String filename) {
	// TODO
    }
    */

    /*
      Appends new data to target file (on all copies).
     */
    public void write(String filename, String new_data) {
	// TODO
    }

    private static Msg communicate(Msg msg)
    {
    	Socket sock;
    	Msg ret_msg = null;
	System.out.println(" [AP] > Trying to reach NameNode at IP " + M_IP + " and port " + Integer.toString(M_Port));
	try {
	    sock = new Socket(M_IP, M_Port);
	    ObjectOutputStream oos = new ObjectOutputStream(sock.getOutputStream());
	    ObjectInputStream ois = new ObjectInputStream(sock.getInputStream());	    
	    oos.writeObject(msg);
	    ret_msg = (Msg)ois.readObject();	    
	    sock.close();
	} catch (UnknownHostException e1) {
	    e1.printStackTrace();
	} catch (IOException e1) {
	    System.out.println(" [AP] > Failed to connect to NameNode");
	}catch (ClassNotFoundException e) {
	    System.out.println(" [AP] > Did not receive ack from NameNode");
    	}	
	return ret_msg;
    }

    public void connect() throws InterruptedException, ClassNotFoundException, UnknownHostException {
	Msg greeting = new Msg();
	greeting.set_msg_type(Constants.MESSAGE_TYPE.CLIENT_GREETING);
	greeting.set_return_address(my_address);
	Msg reply = communicate(greeting);
    }

    public static void main(String args[])
    {

    }

}