import DFS.*;
import UTILS.*;
import java.util.*;

public class test {

    public static void main(String[] args) {

	System.out.println("\n === DFS TESTS === \n");

	/* Chunk Tests */

	System.out.println(" > Creating new data chunk.");
	UTILS.Chunk c = new UTILS.Chunk();
	c.set_filename("File A");
	c.set_chunkID("2");
	c.set_data("Andrea Klein");
	System.out.println(" >>> Chunk filename: " + c.get_filename());
	System.out.println(" >>> Chunk chunkID: " + c.get_chunkID());
	System.out.println(" >>> Chunk data: " + c.get_data());

    }

}