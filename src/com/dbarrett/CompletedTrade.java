package com.dbarrett;

/**
 * Created by dbarrett on 11/29/16.
 */
public class CompletedTrade extends OrderType{
    private String aggressor;
    public CompletedTrade(String ticker, double price, int numShares, String aggressor) {
        super(ticker, price, numShares);
        this.aggressor = aggressor;
    }

    public String getAggressor() {
        return aggressor;
    }

    public void setAggressor(String aggressor) {
        this.aggressor = aggressor;
    }
}
