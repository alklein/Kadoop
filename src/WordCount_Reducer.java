import MR.*;
import DFS.*;
import UTILS.*;

import java.util.*;

public class WordCount_Reducer extends MR.Reducer {

    int port = 1001;
    private AccessPoint ap = new AccessPoint(port);

    public String reduce(ArrayList<String> sorted_filenames, ArrayList<String> sorted_chunkIDs) {

	String result = "";

	String prev = "";
	String cur = "";
	int cur_count = 0;

	UTILS.ChunkName n;
	String data;
	String lines[];
	String sorted_filename;
	String sorted_chunkID;

	for (int i=0; i < sorted_filenames.size(); i++) {
	    sorted_filename = sorted_filenames.get(i);
	    sorted_chunkID = sorted_chunkIDs.get(i);
	 
	    n = new UTILS.ChunkName();
	    n.set_filename(sorted_filename);
	    n.set_chunkID(sorted_chunkID);
	    data = ap.read_chunk(n);
	    lines = data.split("\\r?\\n");

	    for (int j=0; j < lines.length; j++) {
		cur = lines[j];
		if (cur.equals(prev)) { 
		    // word seen again: increment count
		    cur_count += 1;
		} else if (cur_count == 0) { 
		    // beginning end case
		    cur_count += 1;
		} else {
		    // new word: output old word and its total count
		    String output = cur + " " + Integer.toString(cur_count) + "\n";
		    result += output;
		    prev = cur;
		    cur_count = 1;
		}
	    }
	}

	// end edge case:
	String output = cur + " " + Integer.toString(cur_count) + "\n";
	result += output;

	return result;
    }

}
