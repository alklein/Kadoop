package UTILS;

public class Constants {

    // TODO: migrate this into the DFS config file
    public static final String NAMENODE_IP = "0.0.0.0";
    public static final int NAMENODE_PORT = 12345;
   
    public enum MESSAGE_TYPE {
	DATANODE_GREETING, CLIENT_GREETING, GREETING_REPLY, READ_MEM, WRITE_MEM, READ_MEM_REPLY, WRITE_MEM_REPLY, OTHER
	   };	

}
