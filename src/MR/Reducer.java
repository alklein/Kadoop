package MR;

import java.io.*;
import java.util.*;
import java.util.Collections;

public abstract class Reducer {

    /*
    Reduce function to be implemented by client.
    */
    public abstract String reduce(ArrayList<String> sorted_filenames, ArrayList<String> sorted_chunkIDs, boolean verbose);

    /*
    Takes a list of filenames. Each is an output from a mapper.
    Performs an external merge sort on the mapped data to create a single file named sorted_filename.
    */
    private void external_merge_sort(ArrayList<String> mapped_filenames, ArrayList<String> mapped_chunkIDs) {
	// TODO
    }

    /*
    Takes a list of filenames. Each is an output from a mapper.
    Performs an in-memory merge sort on the mapped data to create a single file, single chunk.
    */
    public ArrayList<String> sort(ArrayList<String> lines, boolean verbose) {
	if (verbose) {
	    System.out.println("Lines before sorting:");
	    for (int i=0; i < lines.size(); i++) {
		System.out.println(lines.get(i));
	    }
	}
	Collections.sort(lines);
	if (verbose) {
	    System.out.println("Lines after sorting:");
	    for (int i=0; i < lines.size(); i++) {
		System.out.println(lines.get(i));
	    }
	}
	return lines;
    }

    /*
    Sorts and reduces mapped data.
    */
    public String run(ArrayList<String> mapped_filenames, ArrayList<String> mapped_chunkIDs, ArrayList<String> sorted_filenames, ArrayList<String> sorted_chunkIDs, boolean verbose) {
	external_merge_sort(mapped_filenames, mapped_chunkIDs);
	return reduce(sorted_filenames, sorted_chunkIDs, verbose);
    }

}