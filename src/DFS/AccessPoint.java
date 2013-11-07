package DFS;

import java.util.*;

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

    public static void main(String[] args) {
	
	AccessPoint _ap = AccessPoint.getInstance();
	// TODO: connect to NameNode

    }

}