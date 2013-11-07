package DFS;

import java.util.*;

/*
  Object to be created by any client that wishes to interact with the DFS.
  Manages details of communication with the NameNode and DataNode(s).
 */
public class AccessPoint {

    private static AccessPoint ap = new AccessPoint();

    public static AccessPoint getInstance() {
	return ap;
    }

    /*
      Lists all files available in DFS.
     */
    public ArrayList<String> ls() {
	// TODO
    }

    /*
      Lists all files available on current node.
      For use in MapReduce.
     */
    public ArrayList<String> ls_local() {
	// TODO
    }

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

    public static void main(String[] args) {
	
	AccessPoint _ap = AccessPoint.getInstance();
	// TODO: connect to NameNode

    }

}