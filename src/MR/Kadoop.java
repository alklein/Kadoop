/*

Takes command line input;
Lets you start MR jobs + monitor system

 */

package MR;

import DFS.*;
import UTILS.*;
import UTILS.Constants.*;

import java.net.*;
import java.util.*;

import java.io.Writer;
import java.io.FileWriter;
import java.io.FileReader;
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

public class Kadoop {

    static String IP_file = "Master_IP.txt";
    static String IP;
    static int port;
    
    static String Master_IP;
    static int Master_Port;

    private static Address my_address;
    private static Charset encoding = StandardCharsets.UTF_8;

    private static ObjectOutputStream oos;
    private static ObjectInputStream ois;
    private static Socket sock = null;

    private static Kadoop k = null;
    private static AccessPoint ap = null;

    private static String read_Master_IP() {
	try {
	    String path = IP_file;
	    byte[] encoded = Files.readAllBytes(Paths.get(path));
	    return encoding.decode(ByteBuffer.wrap(encoded)).toString();
	} catch (IOException e) {
            e.printStackTrace();
	}
	return "";
    }

    private Kadoop(int p) {
	String Master_IP = read_Master_IP();
	Master_IP = Master_IP;
	Master_Port = UTILS.Constants.MASTER_PORT;
	try {
	    String IP = InetAddress.getLocalHost().getHostAddress();
	} catch (UnknownHostException e) {
	    e.printStackTrace();
	}
	Address a = new Address();
	a.set_IP(IP);
	a.set_port(p);
	my_address = a;
	ap = new AccessPoint(33333);
    }

    public static Kadoop getInstance(int port) {
	if (k == null) {
	    k = new Kadoop(port);
	}
	return k;
    }   

    /*
      Method to exchange messages with the Master.
      TODO: just send message here. wait for replies via a ServerSocket.
     */
    private static Msg communicate(Msg msg)
    {
    	Socket sock;
    	Msg ret_msg = null;
	try {
	    sock = new Socket(Master_IP, Master_Port);
	    ObjectOutputStream oos = new ObjectOutputStream(sock.getOutputStream());
	    ObjectInputStream ois = new ObjectInputStream(sock.getInputStream());	    
	    oos.writeObject(msg);
	    ret_msg = (Msg) ois.readObject();	    
	    sock.close();
	} catch (UnknownHostException e) {
	    e.printStackTrace();
	} catch (IOException e) {
	    e.printStackTrace();
	} catch (ClassNotFoundException e) {
	    e.printStackTrace();
    	}	
	return ret_msg;
    }

    /*
      Puts chunks into DFS.
      Assumes data file is too large to load into memory,
      but individual chunks are safe to load.
     */
    private static ArrayList<ChunkName> init_data(String data_filename, int num_chunks) throws IOException {	
	ArrayList<ChunkName> result = new ArrayList<ChunkName>();

	int num_lines = 0;
	BufferedReader br = new BufferedReader(new FileReader(data_filename));
	String line;
	while ((line = br.readLine()) != null) {
	    num_lines += 1;
	}
	br.close();
	int lines_per_chunk = num_lines / num_chunks;
	int leftover_lines = num_lines - (num_chunks * lines_per_chunk);

	System.out.println(" >>> Number of chunks: " + Integer.toString(num_chunks));
	System.out.println(" >>> Number of lines in file: " + Integer.toString(num_lines));
	System.out.println(" >>> Lines per chunk: " + Integer.toString(lines_per_chunk));
	System.out.println(" >>> Leftover lines: " + Integer.toString(leftover_lines));

	int chunk_count = 0;
	int line_count;
	BufferedReader br2 = new BufferedReader(new FileReader(data_filename));
	while (chunk_count < num_chunks) {
	    line_count = 0;
	    String chunk_data = "";
	    while (line_count < lines_per_chunk) {
		String new_line = br2.readLine();
		if (line_count < lines_per_chunk - 1) {
		    new_line += "\n";
		}
		chunk_data += new_line;
		line_count += 1;
	    }
	    UTILS.Chunk c = new UTILS.Chunk();
	    UTILS.ChunkName n = new UTILS.ChunkName();
	    n.set_filename(data_filename);
	    n.set_chunkID(Integer.toString(chunk_count));
	    result.add(n);
	    c.set_name(n);
	    c.set_data(chunk_data);
	    ap.write_chunk(c);
	    chunk_count += 1;
	}
	// process leftover lines:
	line_count = 0;
	String chunk_data = "";
	while (line_count < leftover_lines) {
	    String new_line = br2.readLine();
	    if (line_count < leftover_lines - 1) {
		new_line += "\n";
	    }
	    chunk_data += new_line;
	    line_count += 1;
	}
	UTILS.Chunk c = new UTILS.Chunk();
	UTILS.ChunkName n = new UTILS.ChunkName();
	n.set_filename(data_filename);
	n.set_chunkID(Integer.toString(chunk_count));
	result.add(n);
	c.set_name(n);
	c.set_data(chunk_data);
	ap.write_chunk(c);

	return result;
    }

    /*
      Receives messages from the Master.
    */
    public static Msg listen_to_Master() throws IOException, ClassNotFoundException {
	ois = new ObjectInputStream(sock.getInputStream());	
	while (true) { 
	    System.out.println(" [K] > Listening for messages...");
	    Msg msg = (Msg) ois.readObject();
	    System.out.println(" [K] > Received a message!");
	    return msg;
	}
    }

    /*
      Sends message to Master.
    */
    private static void write_to_Master(Msg m) throws IOException, ClassNotFoundException {
	oos.writeObject(m);
	oos.flush();
    }

    // TODO: implement
    private static ArrayList<ChunkName> perform_map(ArrayList<ChunkName> chunk_names, String mapper_classname) {
	ArrayList<ChunkName> result = null;
	Msg msg = new Msg();
	msg.set_msg_type(Constants.MESSAGE_TYPE.ASSIGN_MAPS);
	msg.set_chunk_names(chunk_names);
	msg.set_class_name(mapper_classname);	
	try {
	    write_to_Master(msg);
	    Msg reply = listen_to_Master(); 
	    result = reply.get_chunk_names(); // names of mapped files
	} catch (IOException e) {
            e.printStackTrace();
	} catch (ClassNotFoundException e) {
            e.printStackTrace();
	}
	return result;
    }

    // TODO: implement
    private static String perform_reduce(ArrayList<ChunkName> mapped_chunk_names, String reducer_classname) {
	String result = "";
	return result;
    }

    public static void print_arr_list(ArrayList<String> l) {
	for (int i=0; i < l.size(); i++) {
	    System.out.println("        >>> " + l.get(i));
	}
    }

    /*
      Announces its availability to the Master.
     */
    public void connect() throws InterruptedException, ClassNotFoundException {
	try {
	    System.out.println(" [K] > Attempting to reach Master...");
	    String MASTER_IP = this.read_Master_IP();
	    sock = new Socket(MASTER_IP, UTILS.Constants.MASTER_PORT);
	    oos = new ObjectOutputStream(sock.getOutputStream());
	    Msg greeting = new Msg();
	    greeting.set_msg_type(Constants.MESSAGE_TYPE.COMPUTENODE_GREETING);
	    greeting.set_return_address(my_address);
	    this.write_to_Master(greeting);
	} catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } 
    }


    public static void main(String args[])
    {
	System.out.println(" ~~~ WELCOME TO KADOOP! ~~~");
	Kadoop _k = Kadoop.getInstance(UTILS.Constants.KADOOP_PORT);
	try {
	    _k.connect();
	} catch (InterruptedException e) {
	    e.printStackTrace();
	} catch (ClassNotFoundException e) {
	    e.printStackTrace();
	}
	// TODO: process command-line input from user

	// TEMP:
	String data_filename = "data.txt";
	String mapper_classname = "WordCount_Mapper";
	String reducer_classname = "WordCount_Reducer";
	int num_chunks = 3;

	ArrayList<ChunkName> chunk_names = null;
	ArrayList<ChunkName> mapped_chunk_names = null;
	String outfile_name = null;

	boolean cont = true;
	try {
	    System.out.println(" ~~~ INITIALIZING DATA...");
	    chunk_names = _k.init_data(data_filename, num_chunks);
	    System.out.println(" ~~~ INITIALIZATION COMPLETE. CONTENTS OF DFS:");
	    print_arr_list(ap.ls());
	    // TEMP:
	    /*
	    for (int i=0; i < chunk_names.size(); i++) {
		ChunkName current = chunk_names.get(i);
		String name = current.get_filename();
		String chunk_ID = current.get_chunkID();
		String d = ap.read_chunk(current);
		System.out.println("Filename: " + name + " chunkID: " + chunk_ID + " Data: \n" + d);
	    }
	    */
	} catch (IOException e) {
	    System.out.println(" ~~~ INITIALIZATION FAILED. SHUTTING DOWN COMPUTATION.");
	    cont = false;
	}
	if (cont) {
	    System.out.println(" ~~~ EXECUTING MAP PHASE...");
	    mapped_chunk_names = _k.perform_map(chunk_names, mapper_classname);
	    System.out.println(" ~~~ MAP PHASE COMPLETE. CONTENTS OF DFS:");
	    print_arr_list(ap.ls());
	}
	if (cont) {
	    outfile_name = _k.perform_reduce(mapped_chunk_names, reducer_classname);
	    System.out.println(" ~~~ COMPUTATION COMPLETE. OUTFILE: " + outfile_name);
	}
    }

}