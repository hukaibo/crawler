package com.github.hcsp;

import org.apache.ibatis.io.Resources;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;

import javax.imageio.IIOException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class MyBatisCrawlerDao implements CrawlerDao {
    SqlSessionFactory sqlSessionFactory;

    public MyBatisCrawlerDao() {
   try{
        String resource = "mybatis/config.xml";
        InputStream inputStream = Resources.getResourceAsStream(resource);
         sqlSessionFactory = new SqlSessionFactoryBuilder().build(inputStream);
    }catch (IOException e){
       throw new RuntimeException(e);
   }
    }



    @Override
    public synchronized String getNextLinkThenDelete() throws SQLException {
        try (SqlSession session = sqlSessionFactory.openSession(true)) {
          String url= session.selectOne("com.github.hcsp.MyMapper.selectNextAvailableLink");
          if (url!=null){
              session.delete("com.github.hcsp.MyMapper.deleteLink",url);
          }
          return url;
        }
    }

//    @Override
//    public void updateDatabase(String link, String sql) throws SQLException {
//
//    }

    @Override
    public void insertNewsIntoDatabase(String url, String title, String content) throws SQLException {
        try(SqlSession session = sqlSessionFactory.openSession(true)){
           session.selectOne("com.github.hcsp.MyMapper.insertNews",new News(url, content, title));

        }
    }

    @Override
    public boolean isLinkProcessed(String link) throws SQLException {
        try(SqlSession session = sqlSessionFactory.openSession()){
          int count= session.selectOne("com.github.hcsp.MyMapper.countLink",link);
             return  count!=0;
        }
    }

    @Override
    public void insertProcessedLink(String link) {
        Map<String,Object> param=new HashMap<>();
        param.put("tableName","LINKS_ALREADY_PROCESSED");
        param.put("link", link);
        try(SqlSession session = sqlSessionFactory.openSession(true)){
            session.selectOne("com.github.hcsp.MyMapper.insertLink",param);

        }
    }

    @Override
    public void insertLinkToBeProcessed(String link) {
        Map<String,Object> param=new HashMap<>();
        param.put("tableName","LINKS_TO_BE_PROCESSED");
        param.put("link", link);
        try(SqlSession session = sqlSessionFactory.openSession(true)){
            session.selectOne("com.github.hcsp.MyMapper.insertLink",param);

        }
    }
}