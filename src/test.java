import DFS.*;
import UTILS.*;
import java.util.*;

public class test {

    public static void main(String[] args) {

	System.out.println("\n === DFS TESTS === \n");

	/* Chunk Tests */

	System.out.println(" > Creating new data chunk.");
	UTILS.Chunk c = new UTILS.Chunk();
	UTILS.ChunkName n = new UTILS.ChunkName();
	n.set_filename("File A");
	n.set_chunkID("2");
	c.set_name(n);
	c.set_data("Andrea Klein");
	
	UTILS.ChunkName nn = c.get_name();
	System.out.println(" >>> Chunk filename: " + nn.get_filename());
	System.out.println(" >>> Chunk chunkID: " + nn.get_chunkID());
	System.out.println(" >>> Chunk data: " + c.get_data());

    }

}