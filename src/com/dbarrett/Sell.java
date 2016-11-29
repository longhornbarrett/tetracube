package com.dbarrett;

/**
 * Created by dbarrett on 11/29/16.
 */
public class Sell extends OrderType{

    public Sell(String ticker, double price, int numShares) {
        super(ticker, price, numShares);
    }
}
