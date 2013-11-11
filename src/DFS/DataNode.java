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

public class DataNode {

    static String IP;
    static int port;
    private static Address my_address;
    private static Charset encoding = StandardCharsets.UTF_8;

    private static DataNode dn = null;

    private ObjectOutputStream oos;
    private ObjectInputStream ois;
    private Socket sock = null;

    // list of data chunks this node is managing
    private ArrayList<String> my_files = new ArrayList<String>();

    private DataNode(int port) {
	try {
	    IP = InetAddress.getLocalHost().getHostAddress();
	    Address a = new Address();
	    a.set_IP(IP);
	    a.set_port(port);
	    my_address = a;
	    System.out.println(" [DN] > Got DataNode host address: " + IP);
	} catch (UnknownHostException e) {
	    System.out.println(" [DN] > Failed to get DataNode host address :(");
	}
    }

    public static DataNode getInstance(int port) {
	if (dn == null) {
	    dn = new DataNode(port);
	}
	return dn;
    }   

    public String path_name(ChunkName n) {
	return "/tmp/" + n.toString() + ".txt";
    }

    /*
      Stores a chunk of data locally.
      If file already exists, appends new data to the end.
      May be triggered by a client or e.g. a Reducer.
     */
    public void store(ChunkName n, String data) throws IOException {
	String path = this.path_name(n);
	Writer output;
	output = new BufferedWriter(new FileWriter(path, true));
	output.append(data);
	output.close(); 
    }

    /*
      Reads and returns requested data. 
      Should be called *locally* by e.g. a Mapper or Reducer.
     */
    public String read(ChunkName name) throws IOException  {
	String path = this.path_name(name);
	byte[] encoded = Files.readAllBytes(Paths.get(path));
	return this.encoding.decode(ByteBuffer.wrap(encoded)).toString();
    }
    
    /*
      Parses and processes incoming messages.
     */
    private void process(Msg msg) {
	UTILS.Constants.MESSAGE_TYPE mt = msg.get_msg_type();
	// TODO: respond to message types here
	/*
	if (mt == Constants.MESSAGE_TYPE.DATANODE_GREETING) {
	    System.out.println(" [NN] > Processing DATANODE_GREETING");
	    this.add_node(msg);
	    } */

    }

    /*
      Receives and responds to messages from the NameNode.
     */
    public void listen_to_NN() throws IOException, ClassNotFoundException {
	System.out.println(" [DN] > Listening for messages");
	ois = new ObjectInputStream(sock.getInputStream());	
	while (true) { 
	    Msg msg = (Msg) ois.readObject();
	    System.out.println(" [DN] > Received a message!");
	    this.process(msg);  
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
	    greeting.set_return_address(my_address);
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
	System.out.println(" [DN] > Starting up new DataNode");
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