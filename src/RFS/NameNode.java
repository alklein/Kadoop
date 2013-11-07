import java.util.*;

public class NameNode {

    // maps from the address of each available node to an array list of the file chunks it manages
    private HashMap<String, ArrayList<String>> available_nodes = new HashMap<String, ArrayList<String>>();
    // maps from each file chunk to the actual locations where it resides
    private HashMap<String, ArrayList<String>> data_locations = new HashMap<String, ArrayList<String>>();

    private static NameNode nn = new NameNode();

    public static NameNode getInstance() {
	return nn;
    }   

    /*
      When a new DataNode comes online, adds it to the available nodes.
     */
    private void add_node() {
	// TODO
    }

    /*
      Given a map from file_chunk IDs to memory-loaded data, 
      assigns each chunk to the least busy DataNodes that 
      don't already have it, according to the replication factor
      in the DFS config file. 

      Sends the data to each DataNode and awaits confirmation before
      updating available_nodes and data_locations, lest someone
      request the data right away. 
     */
    private void assign_chunks(HashMap<String, String> data) {
	// TODO
    }

    /*
      Removes failed_node from available_nodes. 
      Recopies the data failed_node was responsible for to other nodes.     
     */
    private void reassign_data(String failed_node) {
	// TODO
    }

    /* 
       Sends a message to each of the nodes in available_nodes. 
       If no reply is heard within some timeout period, the node is
       assumed dead, and its data is reassigned. 
     */
    private void poll() {
	// TODO
    }

    /*
      Returns a list of the available computing nodes.
     */
    /*
    public ArrayList<String> node_list() {
	// TODO
    }
    */

    /*
      Returns a list of all the files in the DFS.
     */
    /*
    public ArrayList<String> file_list() {
	// TODO
    }
    */

    /*
      Returns the actual location of a given file chunk,
      specified in a string formatted as filename_chunkID.
     */
    /*
    public String file_list(String filename_chunkID) {
	// TODO
    }
    */       

    /* 
       Listens for messages from DataNodes and clients.
       Periodically polls the nodes.
     */
    private void listen() {
	// TODO
    }

    public static void main(String[] args) {

	NameNode _nn = NameNode.getInstance();
	_nn.listen();
	System.out.println("I didn't crash!\n");	
    }

}