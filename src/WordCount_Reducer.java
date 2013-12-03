import MR.*;
import DFS.*;
import UTILS.*;

import java.util.*;

public class WordCount_Reducer extends MR.Reducer {

    int port = 1001;
    private AccessPoint ap = new AccessPoint(port);

    public String reduce(ArrayList<String> sorted_filenames, ArrayList<String> sorted_chunkIDs, boolean verbose) {

	String result = "";

	String prev = "";
	String cur = "";
	String cur_word = "";
	String prev_word = "";
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
		String split_line[] = cur.split("\\s+");
		cur_word = split_line[0];
		if (verbose) {
		    System.out.println(" -- Data: " + data); // TEMP
		    System.out.println(" -- Current line: " + cur); // TEMP
		    System.out.println(" -- Current word: " + cur_word + "\n"); // TEMP
		}
		if (cur.equals(prev)) { 
		    // word seen again: increment count
		    cur_count += 1;
		    if (verbose) {
			System.out.println(" ---- Word seen again"); // TEMP
		    }
		} else if (cur_count == 0) { 
		    // beginning end case
		    prev = cur;
		    cur_count += 1;
		    if (verbose) {
			System.out.println(" ---- Beginning edge case"); // TEMP
		    }
		} else {
		    // new word: output old word and its total count, if old word is not blank
		    String prev_split_line[] = prev.split("\\s+");
		    prev_word = prev_split_line[0];
		    if (prev_word.length() > 0) {
			String output = prev_word + " " + Integer.toString(cur_count) + "\n";
			result += output;
		    }
		    prev = cur;
		    cur_count = 1;
		    if (verbose) {
			System.out.println(" ---- Outputting word: " + prev_word); // TEMP
		    }
		}
	    }
	}

	// end edge case:
	String output = cur_word + " " + Integer.toString(cur_count) + "\n";
	result += output;
	if (verbose) {
	    System.out.println(" ---- Outputting word: " + cur_word); // TEMP
	}

	return result;
    }

}
