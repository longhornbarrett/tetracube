package com.dbarrett;

/**
 * Created by dbarrett on 11/13/16.
 */
public class KeyValueOffsets {
    public int keyStart, keyEnd;
    private int valueStart, valueEnd;
    private boolean found;

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

    public int getValueStart() {
        found = false;
        return valueStart;
    }

    public void setValueStart(int valueStart) {
        found = true;
        this.valueStart = valueStart;
    }

    public int getValueEnd() {
        return valueEnd;
    }

    public void setValueEnd(int valueEnd) {
        this.valueEnd = valueEnd;
    }

    public boolean isFound() {
        return found;
    }

    public void setFound(boolean found) {
        this.found = found;
    }
}
