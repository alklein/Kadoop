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
    private static Address my_address;
    private static Charset encoding = StandardCharsets.UTF_8;

    private static AccessPoint ap = new AccessPoint();

    private ObjectOutputStream oos;
    private ObjectInputStream ois;
    private Socket sock = null;

    public static AccessPoint getInstance() {
	return ap;
    }

    private String read_NN_IP() {
	try {
	    String path = this.IP_file;
	    byte[] encoded = Files.readAllBytes(Paths.get(path));
	    return this.encoding.decode(ByteBuffer.wrap(encoded)).toString();
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

    public void connect(int p) throws InterruptedException, ClassNotFoundException, UnknownHostException {
	String IP = InetAddress.getLocalHost().getHostAddress();
	Address a = new Address();
	a.set_IP(IP);
	a.set_port(p);
	this.my_address = a;

	try {
	    System.out.println(" [DN] > Attempting to reach NameNode at IP " + UTILS.Constants.NAMENODE_IP + " and port " + Integer.toString(UTILS.Constants.NAMENODE_PORT));
	    String NAMENODE_IP = this.read_NN_IP();
	    this.sock = new Socket(NAMENODE_IP, UTILS.Constants.NAMENODE_PORT);
	    this.oos = new ObjectOutputStream(sock.getOutputStream());
	    Msg greeting = new Msg();
	    greeting.set_msg_type(Constants.MESSAGE_TYPE.CLIENT_GREETING);
	    greeting.set_return_address(my_address);
	    this.write_to_NN(greeting);
	} catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } 
    }

}