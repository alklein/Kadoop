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

    private ObjectOutputStream oos;
    private ObjectInputStream ois;
    private Socket sock = null;

    private static Kadoop k = null;

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
    }

    public static Kadoop getInstance(int port) {
	if (k == null) {
	    k = new Kadoop(port);
	}
	return k;
    }   

    public static void main(String args[])
    {
	System.out.println(" ~~~ WELCOME TO KADOOP! ~~~");
	Kadoop _k = Kadoop.getInstance(UTILS.Constants.KADOOP_PORT);
	// TODO: process command-line input from user
    }

}