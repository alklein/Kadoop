package DFS;

import java.util.*;

public class DataNode {

    // list of data chunks this node is managing
    private ArrayList<String> my_files = new ArrayList<String>();

    private static DataNode dn = new DataNode();

    public static DataNode getInstance() {
	return dn;
    }   

    /*
      Stores data locally.
     */
    public void store(String filename_chunkID, String data) {
	// TODO
    }

    /*
      Returns requested data.
     */
    /*
    public String data(String filename_chunkID) {
	// TODO
    }
    */

    /*
      Receives and responds to messages from the NameNode.
     */
    public void listen() {
	// TODO
    }

    /*
      Announces its availability to the NameNode.
      Awaits further instruction.
     */
    public void connect() {
	// TODO: announce self to master
    }
    

    public static void main(String[] args) {

	System.out.println("Starting up new DataNode.");
	DataNode _dn = DataNode.getInstance();
	_dn.connect();
	_dn.listen();
	System.out.println("I didn't crash!");	

    }


}