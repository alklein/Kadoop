package UTILS;

import java.util.*;

public class ChunkName implements java.io.Serializable {

    private String Filename;
    private String ChunkID;

    public void set_filename(String filename) {
	this.Filename = filename;
    }

    public String get_filename() {
	return this.Filename;
    } 

    public void set_chunkID(String chunkID) {
	this.ChunkID = chunkID;
    }

    public String get_chunkID() {
	return this.ChunkID;
    }

    public String to_String() {
	return this.Filename + "___" + this.ChunkID;
    }

}