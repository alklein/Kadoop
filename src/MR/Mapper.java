package MR;

import java.io.*;
import java.util.*;

public abstract class Mapper {

    /*
      Map function to be implemented by client.
    */
    public abstract String map(String input_line);

    /*
      Runs map function on every line of file_contents.
      File (chunk) should already be split into ArrayList by ComputeNode.
    */
    public ArrayList<String> run(ArrayList<String> file_contents) {
	ArrayList<String> mapped_file_contents = new ArrayList<String>();
	for (String line : file_contents) {
	    mapped_file_contents.add(map(line));
	}
	return mapped_file_contents;
    }

}