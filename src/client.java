import DFS.*;
import UTILS.*;

import java.io.*;
import java.net.*;
import java.util.*;

public class client {

    public static void print_arr_list(ArrayList<String> l) {
	System.out.println(" >>> Current contents of file system:");
	for (int i=0; i < l.size(); i++) {
	    System.out.println(" >>> " + l.get(i));
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

	ap.write_chunk(c);
	// ap.read_chunk(n);
    }

}