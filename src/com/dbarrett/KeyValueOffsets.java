package com.dbarrett;

/**
 * Created by dbarrett on 11/13/16.
 */
public class KeyValueOffsets {
    public int keyStart, keyEnd, valueStart, valueEnd;

    public boolean isNullOrWhiteSpace()
    {
        if(keyEnd == 0 || valueEnd == 0)
            return true;
        if(keyEnd < keyStart || valueEnd < keyEnd)
            return true;
        return false;
    }

    public int getTagLength()
    {
        return keyEnd - keyStart;
    }
    public int getValueLength(){ return valueEnd - valueStart; }

}
