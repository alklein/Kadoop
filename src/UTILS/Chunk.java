package UTILS;

import java.util.*;

public class Chunk implements java.io.Serializable {

    private ChunkName Name;
    private String Data;

    public void set_name(ChunkName n) {
	this.Name = n;
    }

    public ChunkName get_name() {
	return this.Name;
    }

    public void set_data(String d) {
	this.Data = d;
    }

    public String get_data() {
	return this.Data;
    }

}