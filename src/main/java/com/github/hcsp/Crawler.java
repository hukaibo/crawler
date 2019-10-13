package com.github.hcsp;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Crawler {
    public static void main(String[] args) throws IOException {
        //待处理的连接池
        List<String> linkpool = new ArrayList<>();
        //已经处理的连接池
        Set<String> processedLinks = new HashSet<>();
        linkpool.add("https://sina.cn");

        while (true) {
            if (linkpool.isEmpty()) {
                break;
            }
            String link = linkpool.remove(linkpool.size() - 1);
            //
            if (processedLinks.contains(link)) {
                continue;
            }
            // if (link.contains("news.sina.cn") || "https://sina.cn".equals(link)) {
            if (isInterestingLink(link)) {
                //这是我们感兴趣的，我们只处理新浪的链接
                Document doc = httpGetAndParseHtml(link);
                ArrayList<Element> links = doc.select("a");
                for (Element element : links) {
                    linkpool.add(element.attr("href"));
                }


                storeIntoDatabaseIfItIsNewsPage(doc);
                processedLinks.add(link);


            } else {
                //这是我们不感兴趣的

            }
        }
    }

    private static void storeIntoDatabaseIfItIsNewsPage(Document doc) {
        ArrayList<Element> articles = doc.select("article");
        if (!articles.isEmpty()) {
            for (Element article : articles) {
                String titles = articles.get(0).child(0).text();
                System.out.println(titles);

            }

        }
    }

    private static boolean isInterestingLink(String link) {
        return isNewsPage(link) || isIndexPage(link) && isNotLoginPage(link);

    }

    private static Boolean isNewsPage(String link) {
        return link.contains("news.sina.cn");
    }

    private static Boolean isIndexPage(String link) {
        return "https://sina.cn".equals(link);
    }

    private static Boolean isNotLoginPage(String link) {
        return !link.contains("passport.sina.cn");

    }

    private static Document httpGetAndParseHtml(String link) throws IOException {
        CloseableHttpClient httpclient = HttpClients.createDefault();

        System.out.println(link);
        if (link.startsWith("//")) {
            link = "https:" + link;
        }

        HttpGet httpGet = new HttpGet(link);
        httpGet.addHeader("User-Agent", "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36 SE 2.X MetaSr 1.0");
        try (CloseableHttpResponse response1 = httpclient.execute(httpGet)) {

            System.out.println(response1.getStatusLine());
            HttpEntity entity1 = response1.getEntity();
            // do something useful with the response body
            // and ensure it is fully consumed
            String html = EntityUtils.toString(entity1);
            return Jsoup.parse(html);
        }
    }
}