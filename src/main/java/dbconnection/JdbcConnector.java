package dbconnection;

import java.io.IOException;
import java.sql.*;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public abstract class JdbcConnector {

    protected static JdbcConnector instance;
    protected String jdbcConn;
    protected Connection conn;
    protected int fetchSize;

    public void connect(String jdbcConn, String username, String password) throws SQLException, IOException {
        this.jdbcConn = jdbcConn;

        // if the connection is kerberised
        // https://www.ibm.com/support/knowledgecenter/en/SSPT3X_3.0.0/com.ibm.swg.im.infosphere.biginsights.admin.doc/doc/kerberos_hive.html
        if(jdbcConn.contains("principal")){
            if(username != null && password != null){
                conn = DriverManager.getConnection(jdbcConn, username, password);
            }else{
                conn = DriverManager.getConnection(jdbcConn);
            }

        }else{
            // non-kerberised connecton
            conn = DriverManager.getConnection(jdbcConn, username, password);
        }

        // default fetchSize is 50
        fetchSize = 50;
    }

    public void setFetchSize(int fetchSize){
        this.fetchSize = fetchSize;
    }

    public ResultSet executeQuery(String query) throws SQLException{
        Statement stmt = conn.createStatement();
        stmt.setFetchSize(fetchSize);
        ResultSet res = stmt.executeQuery(query);
        //stmt.close();
        return res;
    }

    public int executeInsertOrUpdate(String query) throws SQLException{
        Statement stmt = conn.createStatement();
        int res =  stmt.executeUpdate(query);
        //stmt.close();
        return res;
    }

    public int[] executeBulkQuery(List<String> queries) throws SQLException{
        int[] res = null;
        if(queries.size() > 0){
            Statement stmt = conn.createStatement();

            //System.out.println("Preparing query batch to be run:");
            for(String q : queries){
                //System.out.println(q);
                stmt.addBatch(q);
            }
            // execute the batch
            res = stmt.executeBatch();
            // count successfull queries
            int s = 0;
            for(int i : res){
                if(i > 0 || i == Statement.SUCCESS_NO_INFO) s++;
            }
            stmt.close();
            System.out.println("Sent a batch of "+queries.size()+" queries, "+s+" were successfully taken");
        }else{
            System.out.println("No queries to be batched");
        }
        return res;
    }

    public void executeQueryUsingPreparedStatement(String genericQuery, List<LinkedHashMap<String, Object>> parameters) throws SQLException{
        PreparedStatement prstmt = conn.prepareStatement(genericQuery);

        conn.setAutoCommit(false);

        for(Map<String,Object> entry : parameters){
            int i_k=0;
            for(String k : entry.keySet()){
                prstmt.setObject(i_k, entry.get(k));
                i_k++;
            }
            prstmt.execute();
        }

        conn.commit();
    }

}
