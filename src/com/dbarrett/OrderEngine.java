package com.dbarrett;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Queue;

/**
 * Created by dbarrett on 11/29/16.
 */
public class OrderEngine {
    private HashMap<String, Queue<Buy>> standingBuyOffers = new HashMap<String, Queue<Buy>>();
    private HashMap<String, Queue<Sell>> standingSellOffers = new HashMap<String, Queue<Sell>>();
    private ArrayList<CompletedTrade> completedTrades = new ArrayList<CompletedTrade>();
    public void matchBuy(Buy buy){
        Queue<Sell> standingSellOrderForTicker = standingSellOffers.get(buy.getTicker());
        if(standingSellOrderForTicker == null)
        {
            storeBuy(buy);
        }else {
            executeBuy(buy, standingSellOrderForTicker);
        }
    }

    private void storeBuy(Buy buy)
    {
        Queue buysForTicker = standingBuyOffers.get(buy.getTicker());
        if(buysForTicker == null) {
            buysForTicker = new LinkedList<Buy>();
            standingBuyOffers.put(buy.getTicker(), buysForTicker);
        }
        buysForTicker.add(buy);
    }

    private boolean executeBuy(Buy buy, Queue<Sell> standingSellOrdersForTicker)
    {
        boolean finishedTrade = false;
        Sell matchingSell = standingSellOrdersForTicker.peek();
        if(matchingSell == null)
        {
            storeBuy(buy);
            finishedTrade = true;
        }else if(matchingSell.getNumShares() < buy.getNumShares())
        {
            CompletedTrade trade = new CompletedTrade(matchingSell.getTicker(), buy.getPrice(), matchingSell.getNumShares(), OrderBook.BUY);
            standingSellOrdersForTicker.remove();
            completedTrades.add(trade);
            buy.setNumShares(buy.getNumShares()-trade.getNumShares());
        }else if(matchingSell.getNumShares() == buy.getNumShares())
        {
            CompletedTrade trade = new CompletedTrade(matchingSell.getTicker(), buy.getPrice(), matchingSell.getNumShares(), OrderBook.BUY);
            standingSellOrdersForTicker.remove();
            completedTrades.add(trade);
            finishedTrade = true;
        }else
        {
            CompletedTrade trade = new CompletedTrade(matchingSell.getTicker(), buy.getPrice(), buy.getNumShares(), OrderBook.BUY);
            completedTrades.add(trade);
            matchingSell.setNumShares(matchingSell.getNumShares()-trade.getNumShares());
            finishedTrade = true;
        }
        return finishedTrade;
    }

    private boolean executeSell(Sell sell, Queue<Buy> standingBuyOrdersForTicker)
    {
        boolean finishedTrade = false;
        Buy matchingBuy = standingBuyOrdersForTicker.peek();
        if(matchingBuy == null)
        {
            storeSell(sell);
            finishedTrade = true;
        }else if(matchingBuy.getNumShares() < sell.getNumShares())
        {
            CompletedTrade trade = new CompletedTrade(matchingBuy.getTicker(), sell.getPrice(), matchingBuy.getNumShares(), OrderBook.SELL);
            standingBuyOrdersForTicker.remove();
            completedTrades.add(trade);
            sell.setNumShares(sell.getNumShares()-trade.getNumShares());
        }else if(matchingBuy.getNumShares() == sell.getNumShares())
        {
            CompletedTrade trade = new CompletedTrade(matchingBuy.getTicker(), sell.getPrice(), matchingBuy.getNumShares(), OrderBook.SELL);
            standingBuyOrdersForTicker.remove();
            completedTrades.add(trade);
            finishedTrade = true;
        }else
        {
            CompletedTrade trade = new CompletedTrade(matchingBuy.getTicker(), sell.getPrice(), sell.getNumShares(), OrderBook.SELL);
            completedTrades.add(trade);
            matchingBuy.setNumShares(matchingBuy.getNumShares()-trade.getNumShares());
            finishedTrade = true;
        }
        return finishedTrade;
    }


    public void matchSell(Sell sell)
    {
        Queue standingBuyOrderForTicker = standingBuyOffers.get(sell.getTicker());
        if(standingBuyOrderForTicker == null)
        {
            storeSell(sell);
        }else {
            executeSell(sell, standingBuyOrderForTicker);
        }
    }

    private void storeSell(Sell sell)
    {
        Queue sellsForTicker = standingSellOffers.get(sell.getTicker());
        if(sellsForTicker == null) {
            sellsForTicker = new LinkedList<Sell>();
            standingBuyOffers.put(sell.getTicker(), sellsForTicker);
        }
        sellsForTicker.add(sell);
    }

}
