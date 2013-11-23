package MR;

import java.io.*;
import java.util.*;

public abstract class Reducer {

    /*
    Reduce function to be implemented by client.
    */
    public abstract String reduce(ArrayList<String> sorted_filename, ArrayList<String> sorted_chunkIDs);

    /*
    Takes a list of filenames. Each is an output from a mapper.
    Performs an external merge sort on the mapped data to create a single file named sorted_fileanme.
    */
    private void external_merge_sort(ArrayList<String> mapped_filenames, ArrayList<String> mapped_chunkIDs) {
	// TODO
    }

    /*
    Sorts and reduces mapped data.
    */
    public String run(ArrayList<String> mapped_filenames, ArrayList<String> mapped_chunkIDs, ArrayList<String> sorted_filenames, ArrayList<String> sorted_chunkIDs) {
	external_merge_sort(mapped_filenames, mapped_chunkIDs);
	return reduce(sorted_filenames, sorted_chunkIDs);
    }

}