package com.github.hcsp;

import java.sql.*;

public class JdbcCrawlerDao implements CrawlerDao{
    private static final String USER_NAME = "root";
    private static final String PASSWORD = "root";
    private final Connection connection;

    public JdbcCrawlerDao() {
        try {
           this.connection = DriverManager.getConnection("jdbc:h2:file:C:\\Users\\胡凯波\\IdeaProjects\\crawler\\news", USER_NAME, PASSWORD);
        } catch (SQLException e) {
            throw new RuntimeException();
        }
    }

    private String getNextLink(String sql) throws SQLException {
        ResultSet resultSet = null;
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            resultSet = statement.executeQuery();
            while (resultSet.next()) {
                return resultSet.getString(1);
            }
        }finally {
            if (resultSet!=null){
                resultSet.close();
            }
        }
        return null;
    }
    public  String getNextLinkThenDelete() throws SQLException {
        String link=   getNextLink("select link from LINKS_TO_BE_PROCESSED LIMIT 1");
        if (link!=null){
            updateDatabase( link, "delete  from LINKS_TO_BE_PROCESSED where link=?");

        }
        return link;
    }
    public   void updateDatabase( String link, String sql) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, link);
            statement.executeUpdate();
        }
    }
    public void insertNewsIntoDatabase(String url,String title,String content) throws SQLException {
        try (PreparedStatement statement=connection.prepareStatement("insert into news (url,title,content,created_at,modified_at)values (?,?,?,now(),now())")){
            statement.setString(1,url);
            statement.setString(2,title);
            statement.setString(3,content);
            statement.executeUpdate();
        }
    }
    public  boolean isLinkProcessed( String link) throws SQLException {
        ResultSet resultSet = null;
        try (PreparedStatement statement = connection.prepareStatement("select LINK from LINKS_ALREADY_PROCESSED where link=?")) {
            statement.setString(1, link);
            resultSet = statement.executeQuery();
            while (resultSet.next()) {
                return true;
            }
        } finally {
            if (resultSet != null) {
                resultSet.close();
            }
        }
        return false;
    }

    @Override
    public void insertProcessedLink(String link) {

    }

    @Override
    public void insertLinkToBeProcessed(String href) {

    }
}
