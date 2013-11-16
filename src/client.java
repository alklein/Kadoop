import DFS.*;
import UTILS.*;

import java.io.*;
import java.net.*;
import java.util.*;

public class client {

    public static void print_arr_list(ArrayList<String> l) {
	System.out.println(" [CLIENT] > Current contents of file system:");
	for (int i=0; i < l.size(); i++) {
	    System.out.println("        >>> " + l.get(i));
	}
    }

    public static void main(String[] args) throws InterruptedException, ClassNotFoundException, UnknownHostException {

	int port = 1001;
	AccessPoint ap = new AccessPoint(port);
	ap.greet_NN();

	ArrayList<String> file_list = ap.ls();
	print_arr_list(file_list);

	UTILS.Chunk c = new UTILS.Chunk();
	UTILS.ChunkName n = new UTILS.ChunkName();
	n.set_filename("File A");
	n.set_chunkID("0");
	c.set_name(n);
	c.set_data("Andrea Klein");

	System.out.println(" [CLIENT] > Writing File A, chunk 0");
	ap.write_chunk(c);

	n.set_chunkID("1");
	c.set_name(n);
	System.out.println(" [CLIENT] > Writing File A, chunk 1");
	ap.write_chunk(c);

	n.set_filename("File B");
	n.set_chunkID("0");
	c.set_name(n);
	System.out.println(" [CLIENT] > Writing File A, chunk 0");
	ap.write_chunk(c);
	
	print_arr_list(ap.ls());

	n.set_filename("File A");
	n.set_chunkID("0");
	String d = ap.read_chunk(n);
	System.out.println(" [CLIENT] > Data read back from DFS: " + d);
    }

}