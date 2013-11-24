package UTILS;

public class Constants {

    // TODO: migrate this into the DFS config file
    //public static final String NAMENODE_IP = "0.0.0.0";
    public static final int NAMENODE_PORT = 15440;
    public static final int MASTER_PORT = 15640;
    public static final int KADOOP_PORT = 11111;
   
    public enum MESSAGE_TYPE {
	DATANODE_GREETING, CLIENT_GREETING, COMPUTENODE_GREETING, GREETING_REPLY, 
	    MAP, MAP_REPLY, 
	    REDUCE, REDUCE_REPLY,
	    ASSIGN_MAPS, ASSIGN_MAPS_REPLY,
	    ASSIGN_REDUCES, ASSIGN_REDUCES_REPLY,
	    WRITE, WRITE_REPLY, 
	    READ_CHUNK, READ_CHUNK_REPLY,
	    READ_MEM, WRITE_MEM, READ_MEM_REPLY, WRITE_MEM_REPLY, 
	    LS, LS_REPLY, OTHER
	    };	
    
}
