package UTILS;

import java.util.*;

public class Address implements java.io.Serializable {

    private String IP;
    private int Port;

    public void set_IP(String ip) {
	this.IP = ip;
    }

    public String get_IP() {
	return this.IP;
    }

    public void set_port(int port) {
	this.Port = port;
    }

    public int get_port() {
	return this.Port;
    }

}