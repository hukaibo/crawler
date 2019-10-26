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

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.stream.Collectors;

//使用flyway先清理，再迁移  mvn flyway:clean flyway:migrate
public class Crawler extends Thread {

    private CrawlerDao dao;

    public Crawler(CrawlerDao dao) {
        this.dao = dao;
    }

    @Override
    public void run() {
        try {
            //待处理的连接池
            String link;
            while ((link = dao.getNextLinkThenDelete()) != null) {
                //先从数据库里拿出一个链接（拿出来并从数据库中删除），并处理之。


                if (dao.isLinkProcessed(link)) {
                    continue;
                }

                if (isInterestingLink(link)) {
                    System.out.println(link);
                    //这是我们感兴趣的，我们只处理新浪的链接

                    Document doc = httpGetAndParseHtml(link);
                    parseUrlsFromPageAndStoreIntoDatabase(doc);
                    storeIntoDatabaseIfItIsNewsPage(doc, link);

                    dao.insertProcessedLink(link);
                    // dao.updateDatabase( link, "insert into LINKS_ALREADY_PROCESSED (link)values (?)");

                }
            }
        }catch (Exception e){
             throw new RuntimeException();
        }
    }



    private void parseUrlsFromPageAndStoreIntoDatabase(Document doc) throws SQLException {
        for (Element aTag : doc.select("a")) {
            String href = aTag.attr("href");

            if (href.startsWith("//")) {
                href = "https:" + href;
            }


            if (!href.toLowerCase().startsWith("javascript")) {
                dao.insertLinkToBeProcessed(href);
                //dao.updateDatabase( href, "insert into LINKS_TO_BE_PROCESSED (link)values (?)");
            }
        }
    }


    private void storeIntoDatabaseIfItIsNewsPage(Document doc, String link) throws SQLException {
        ArrayList<Element> articleTags = doc.select("article");
        if (!articleTags.isEmpty()) {
            for (Element articleTag : articleTags) {
                String titles = articleTags.get(0).child(0).text();
                String content = articleTag.select("p").stream().map(Element::text).collect(Collectors.joining("\n"));
                dao.insertNewsIntoDatabase(link, titles, content);
            }

        }
    }

    private static boolean isInterestingLink(String link) {
        return (isNewsPage(link) || isIndexPage(link)) && isNotLoginPage(link);

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


        HttpGet httpGet = new HttpGet(link);
        httpGet.addHeader("User-Agent", "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36 SE 2.X MetaSr 1.0");
        try (CloseableHttpResponse response1 = httpclient.execute(httpGet)) {

            //  System.out.println(response1.getStatusLine());
            HttpEntity entity1 = response1.getEntity();
            // do something useful with the response body
            // and ensure it is fully consumed
            String html = EntityUtils.toString(entity1);
            return Jsoup.parse(html);
        }
    }
}