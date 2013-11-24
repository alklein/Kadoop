package UTILS;

import UTILS.Constants.MESSAGE_TYPE;
import java.util.*;

public class Msg implements java.io.Serializable {

    private MESSAGE_TYPE msg_type = null;
    private Address return_address = null;
    private ChunkName name = null;
    private String data = null;
    private ArrayList<String> arr_list = null;
    private ArrayList<ChunkName> chunk_names = null;
    private String class_name = null;

    public void set_msg_type(MESSAGE_TYPE tp)
    {
    	this.msg_type = tp;
    }
    
    public MESSAGE_TYPE get_msg_type()
    {
    	return this.msg_type;
    }

    public void set_return_address(Address ra) {
	this.return_address = ra;
    }

    public Address get_return_address() {
	return this.return_address;
    }

    public void set_chunk_name(ChunkName n) {
	this.name = n;
    }

    public ChunkName get_chunk_name() {
	return this.name;
    }

    public void set_data(String d) {
	this.data = d;
    }

    public String get_data() {
	return this.data;
    }

    public void set_arr_list(ArrayList<String> al) {
	this.arr_list = al;
    }

    public ArrayList<String> get_arr_list() {
	return this.arr_list;
    }

    public void set_chunk_names(ArrayList<ChunkName> cns) {
	this.chunk_names = cns;
    }

    public ArrayList<ChunkName> get_chunk_names() {
	return this.chunk_names;
    }

    public void set_class_name(String cn) {
	this.class_name = cn;
    }

    public String get_class_name() {
	return this.class_name;
    }

}