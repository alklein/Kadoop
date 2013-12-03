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

    /*
      Performs map on data; returns result.
     */
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
      Performs reduce on data; saves result to file system.
      Returns name of resulting file chunk.
     */
    private ChunkName perform_reduce(ArrayList<ChunkName> mapped_chunk_names, String class_name) {
	ArrayList<String> lines = new ArrayList<String>();
	for (int i=0; i < mapped_chunk_names.size(); i++) {
	    ChunkName cur = mapped_chunk_names.get(i);
	    String data = ap.read_chunk(cur);
	    String cur_lines[] = data.split("\\r?\\n");
	    for (int j=0; j < cur_lines.length; j++) {
		String cur_str = cur_lines[j];
		lines.add(cur_str);
	    }	    
	}
	try {
	    Class myClass = Class.forName(class_name);
	    Reducer r = (Reducer)myClass.newInstance();
	    ArrayList<String> sorted_lines = r.sort(lines);

	    ChunkName sorted_name = mapped_chunk_names.get(0);
	    String old_filename = sorted_name.get_filename();
	    String sorted_filename = old_filename + "_sorted";
	    sorted_name.set_filename(sorted_filename);

	    String data = "";
	    for (int i=0; i < lines.size(); i++) {
		data += lines.get(i) + "\n";
	    }
	    Chunk c = new Chunk();
	    c.set_name(sorted_name);
	    c.set_data(data);
	    ap.write_chunk(c);
	    
	    ArrayList<String> sorted_filenames = new ArrayList<String>();
	    ArrayList<String> sorted_chunkIDs = new ArrayList<String>();
	    sorted_filenames.add(sorted_filename);
	    sorted_chunkIDs.add(sorted_name.get_chunkID());
	    String reduced_data = r.reduce(sorted_filenames, sorted_chunkIDs);

	    ChunkName reduced_name = sorted_name;
	    reduced_name.set_filename(sorted_name.get_filename() + "_reduced");
	    Chunk cc = new Chunk();
	    cc.set_name(reduced_name);
	    cc.set_data(reduced_data);
	    ap.write_chunk(cc);
	    return reduced_name;
	} catch (ClassNotFoundException e) {
	    System.out.println("Couldn't find class :(");
	} catch (InstantiationException e) {
	    e.printStackTrace();
	} catch (IllegalAccessException e) {
	    e.printStackTrace();
	}
	return null;
    }

    /*
      Parses and processes incoming messages.
     */
    private void process(Msg msg) {
	UTILS.Constants.MESSAGE_TYPE mt = msg.get_msg_type();
	if (mt == Constants.MESSAGE_TYPE.MAP) {
	    // apply mapper class
	    System.out.println(" [CN] > Processing MAP");	    
	    Address ret_add = msg.get_return_address();
	    ChunkName n = msg.get_chunk_name();
	    String class_name = msg.get_class_name();
	    String data = ap.read_chunk(n);
	    String result = this.perform_map(data, class_name);
	    System.out.println(" [CN] Result of map: " + result); // TEMP
	    ChunkName new_name = n;
	    new_name.set_filename(n.get_filename() + "_mapped");
	    Chunk c = new Chunk();
	    c.set_name(new_name);
	    c.set_data(result);
	    ap.write_chunk(c);
	    
	    // reply to master
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
	if (mt == Constants.MESSAGE_TYPE.REDUCE) {
	    // perform sort first; then apply reducer class
	    System.out.println(" [CN] > Processing REDUCE");	    
	    Address ret_add = msg.get_return_address();
	    ArrayList<ChunkName> mapped_chunk_names = msg.get_chunk_names();
	    ChunkName n = mapped_chunk_names.get(0);
	    String class_name = msg.get_class_name();
	    ChunkName new_name = this.perform_reduce(mapped_chunk_names, class_name);

	    // reply to master
	    Msg reply = new Msg();
	    reply.set_msg_type(Constants.MESSAGE_TYPE.REDUCE_REPLY);
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