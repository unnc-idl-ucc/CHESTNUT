package com.chestnut.runtime.IMDb.exports;

import java.io.IOException;

import org.jsoup.HttpStatusException;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;

import java.net.SocketException;
import java.net.SocketTimeoutException;

public class ContentsCrawler {

    private Document _pageDom;
    
    public ContentsCrawler() {
        
    }
    
    public void SetPageDom(String url) {
        try {
            try {
                _pageDom = Jsoup.connect(url).get();
                System.out.println("[ContentsCrawler.SetPageDom]: page, " + url + ", set.");
            }catch(SocketException|SocketTimeoutException se) {
                try {
                    Thread.sleep(10000);
                    SetPageDom(url);
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }catch(HttpStatusException he) {
                System.out.println("HttpError-" + he.getStatusCode());
            }
            
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    
    public String GetPosterUrl() {
        if(_pageDom != null) {
            Elements posterDiv = _pageDom.getElementsByClass("poster");
            if(posterDiv.size()==0) {
                return "not exist";
            }else {
                return posterDiv.get(0).getElementsByTag("img").get(0).attr("src");
            }
        }else {
            return "dom not exist";
        }
        
    }
    
}
