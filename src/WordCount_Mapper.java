import MR.*;
import DFS.*;
import UTILS.*;

public class WordCount_Mapper extends MR.Mapper {

    /*
      Client implementation of map function.
      Assumes each word is on its own line; outputs word and its count (always 1).
    */
    public String map(String input_line) {
	return input_line + " 1";
    }

}