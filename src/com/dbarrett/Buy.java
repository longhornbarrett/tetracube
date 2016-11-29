package com.dbarrett;

/**
 * Created by dbarrett on 11/29/16.
 */
public class Buy extends OrderType{

    public Buy(String ticker, double price, int numShares) {
        super(ticker, price, numShares);
    }
}
