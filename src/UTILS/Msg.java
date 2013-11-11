package UTILS;

import UTILS.Constants.MESSAGE_TYPE;

public class Msg implements java.io.Serializable {

    private MESSAGE_TYPE msg_type = null;

    public void set_msg_type(MESSAGE_TYPE tp)
    {
    	this.msg_type = tp;
    }
    
    public MESSAGE_TYPE get_msg_type()
    {
    	return this.msg_type;
    }

}