package com.dbarrett;

/**
 * Created by dbarrett on 11/29/16.
 */
public class OrderType implements Comparable{
    private String ticker;
    private Double price;
    private int numShares;

    public OrderType(String ticker, double price, int numShares) {
        this.ticker = ticker;
        this.price = price;
        this.numShares = numShares;
    }

    public String getTicker() {
        return ticker;
    }

    public void setTicker(String ticker) {
        this.ticker = ticker;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    public int getNumShares() {
        return numShares;
    }

    public void setNumShares(int numShares) {
        this.numShares = numShares;
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj);
    }

    @Override
    public int compareTo(Object o) {
        OrderType right = (OrderType)o;
        if(this.price.compareTo(right.price) == 0)
            return this.price.compareTo(right.price);
        return -1;
    }
}
