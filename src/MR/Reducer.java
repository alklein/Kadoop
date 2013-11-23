package MR;

import java.io.*;
import java.util.*;

public abstract class Reducer {

    /*
    Reduce function to be implemented by client.
    */
    public abstract String reduce(String sorted_filename);

    /*
    Takes a list of filenames. Each is an output from a mapper.
    Performs an external merge sort on the mapped data to create a single file named sorted_fileanme.
    */
    private void external_merge_sort(ArrayList<String> mapped_filenames, String sorted_filename) {
	// TODO
    }

    /*
    Sorts and reduces mapped data.
    */
    public String run(ArrayList<String> mapped_file_contents, String sorted_filename) {
	external_merge_sort(mapped_file_contents, sorted_filename);
	return reduce(sorted_filename);
    }

}