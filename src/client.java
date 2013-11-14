import DFS.*;
import UTILS.*;

import java.io.*;
import java.net.*;
import java.util.*;

public class client {

    public static void main(String[] args) throws InterruptedException, ClassNotFoundException, UnknownHostException {

	int port = 1001;
	AccessPoint ap = new AccessPoint(port);
	ap.connect();

    }

}