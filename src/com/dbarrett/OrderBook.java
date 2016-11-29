package com.dbarrett;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.*;

/**
 * Created by dbarrett on 11/29/16.
 */
public class OrderBook {
    public static String BUY = "buy";
    public static String SELL = "sell";


    public OrderBook()
    {

    }

    public void match(String input, String orderBook, String trades)
    {
        try {
            OrderEngine orderEngine = new OrderEngine();
            BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(input)));
            String line = "";
            while((line = reader.readLine()) != null)
            {
                try {
                    StringTokenizer st = new StringTokenizer(line, ",");
                    if (st.countTokens() < 4)
                        continue;
                    String ticker = st.nextToken();
                    String price = st.nextToken();
                    String shares = st.nextToken();
                    String orderType = st.nextToken();
                    Double priceD = 0.0;
                    if(price.charAt(0) == '$')
                        priceD = Double.parseDouble(price.substring(1));
                    else
                        priceD = Double.parseDouble(price);
                    if(orderType.toLowerCase().equals(BUY))
                    {
                       orderEngine.matchBuy(new Buy(ticker, priceD, Integer.parseInt(shares)));


                    }else if(orderType.toLowerCase().equals(SELL))
                    {
                        orderEngine.matchSell(new Sell(ticker, priceD, Integer.parseInt(shares)));

                    }
                }catch (Exception e)
                {
                   e.printStackTrace();
                }
            }
            reader.close();
        }catch (Exception e)
        {
            e.printStackTrace();
        }

    }

    public static void main(String[] args)
    {
        String input = "./simpleOrders.csv";
        String orderBook = "./orderBook.txt";
        String trades = "./trades.csv";
        if(args.length > 2)
        {
            input = args[0];
            orderBook = args[1];
            trades = args[2];
        }
        OrderBook orders = new OrderBook();
        orders.match(input, orderBook, trades);
    }
}
