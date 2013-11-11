package UTILS;

import java.util.*;

public class Chunk implements java.io.Serializable {

    private String Filename;
    private String ChunkID;
    private String Data;

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

    public void set_data(String data) {
	this.Data = data;
    }

    public String get_data() {
	return this.Data;
    }

}