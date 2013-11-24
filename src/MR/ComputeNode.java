/*

Should start up + notify Master
Notify Master whenever idle
Should be able to request files locally or remotely via DFS
Should be able to execute map or reduce tasks
   (compiled map / reduce class available via AFS;
   just send filename in message)

 */

package MR;

import DFS.*;
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

public class ComputeNode {

    static String IP_file = "Master_IP.txt";
    static String IP;
    static int port;
    private static Address my_address;
    private static Charset encoding = StandardCharsets.UTF_8;
    private static AccessPoint ap;

    private static ComputeNode cn = null;

    private ObjectOutputStream oos;
    private ObjectInputStream ois;
    private Socket sock = null;

    private String read_Master_IP() {
	try {
	    String path = this.IP_file;
	    byte[] encoded = Files.readAllBytes(Paths.get(path));
	    return this.encoding.decode(ByteBuffer.wrap(encoded)).toString();
	} catch (IOException e) {
            e.printStackTrace();
	}
	return "";
    }

    private ComputeNode(int port) {
	try {
	    IP = InetAddress.getLocalHost().getHostAddress();
	    Address a = new Address();
	    a.set_IP(IP);
	    a.set_port(port);
	    my_address = a;
	    ap = new AccessPoint(port + 1);
	    System.out.println(" [CN] > Got ComputeNode host address: " + IP);
	} catch (UnknownHostException e) {
	    System.out.println(" [CN] > Failed to get ComputeNode host address :(");
	}
    }

    public static ComputeNode getInstance(int port) {
	if (cn == null) {
	    cn = new ComputeNode(port);
	}
	return cn;
    }   

    /*
      Sends message to Master.
    */
    private void write_to_Master(Msg m) throws IOException, ClassNotFoundException {
	oos.writeObject(m);
	oos.flush();
    }

    private String perform_map(String data, String class_name) {
	String lines[] = data.split("\\r?\\n");
	String result = "";
	try {
	    Class myClass = Class.forName(class_name);
	    Mapper m = (Mapper)myClass.newInstance();
	    for (int i=0; i < lines.length; i++) {
		String mapped_line = m.map(lines[i]);
		if (i < lines.length - 1) {
		    mapped_line += "\n";
		}
		result += mapped_line;
	    }
	} catch (ClassNotFoundException e) {
	    System.out.println("Couldn't find class :(");
	} catch (InstantiationException e) {
	    e.printStackTrace();
	} catch (IllegalAccessException e) {
	    e.printStackTrace();
	}
	return result;
    } 

    /*
      Parses and processes incoming messages.
     */
    private void process(Msg msg) {
	UTILS.Constants.MESSAGE_TYPE mt = msg.get_msg_type();
	if (mt == Constants.MESSAGE_TYPE.MAP) {
	    System.out.println(" [CN] > Processing MAP");	    
	    Address ret_add = msg.get_return_address();
	    ChunkName n = msg.get_chunk_name();
	    String class_name = msg.get_class_name();
	    String data = ap.read_chunk(n);
	    String result = perform_map(data, class_name);
	    ChunkName new_name = n;
	    new_name.set_filename(n.get_filename() + "_mapped");

	    Msg reply = new Msg();
	    reply.set_msg_type(Constants.MESSAGE_TYPE.MAP_REPLY);
	    reply.set_return_address(my_address);
	    reply.set_chunk_name(new_name);
	    try {
		this.write_to_Master(reply);
	    } catch (IOException e) {
		e.printStackTrace();
	    }
	    catch (ClassNotFoundException e) {
		e.printStackTrace();
	    }
	} 
    }

    /*
      Receives and responds to messages from the Master.
    */
    public void listen_to_Master() throws IOException, ClassNotFoundException {
	ois = new ObjectInputStream(sock.getInputStream());	
	while (true) { 
	    System.out.println(" [CN] > Listening for messages...");
	    Msg msg = (Msg) ois.readObject();
	    System.out.println(" [CN] > Received a message!");
	    this.process(msg);  
	}
    }

    /*
      Announces its availability to the Master.
      Awaits further instruction.
     */
    public void connect() throws InterruptedException, ClassNotFoundException {
	try {
	    System.out.println(" [CN] > Attempting to reach Master...");
	    String MASTER_IP = this.read_Master_IP();
	    sock = new Socket(MASTER_IP, UTILS.Constants.MASTER_PORT);
	    oos = new ObjectOutputStream(sock.getOutputStream());
	    Msg greeting = new Msg();
	    greeting.set_msg_type(Constants.MESSAGE_TYPE.COMPUTENODE_GREETING);
	    greeting.set_return_address(my_address);
	    this.write_to_Master(greeting);
	    this.listen_to_Master(); 
	} catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } 
    }


    public static void main(String[] args) {

	int hard_port = 20000; // TODO: get from config file
	System.out.println(" [CN] > Starting up new ComputeNode");
	ComputeNode _cn = ComputeNode.getInstance(hard_port);
	try {
	    _cn.connect();
	} catch (InterruptedException e) {
	    e.printStackTrace();
	} catch (ClassNotFoundException e) {
	    e.printStackTrace();
	}
    }




}