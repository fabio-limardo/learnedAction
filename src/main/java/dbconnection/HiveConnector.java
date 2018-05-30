package dbconnection;

import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

public class HiveConnector extends JdbcConnector {
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";

    public static JdbcConnector getInstance(HiveConnectionProperties prop) throws ClassNotFoundException, IOException, SQLException {
        if(instance == null){
            instance = new HiveConnector(prop);
        }
        return instance;
    }

    private HiveConnector(HiveConnectionProperties pref) throws ClassNotFoundException, IOException, SQLException{
        org.apache.hadoop.conf.Configuration conf = new     org.apache.hadoop.conf.Configuration();
        conf.set("hadoop.security.authentication", "kerberos");

        UserGroupInformation.setConfiguration(conf);
        UserGroupInformation.loginUserFromKeytab(pref.getHive_ktab_user(),pref.getHive_ktab_path());
        Class.forName(driverName);
        conn  = DriverManager.getConnection(pref.getHive_connection_string(), pref.getHive_username(), pref.getHive_password());



//	      UserGroupInformation.loginUserFromKeytab("sqoop@SKY.LOCAL", "/opt/kerberos/keytabs/sqoop.keytab");
//	      Class.forName(driverName);
//	      conn  = DriverManager.getConnection("jdbc:hive2://mitstatlodpdata01.sky.local:10000/default;"
//		      		+ "transportMode=http;httpPath=cliservice;principal=hive/mitstatlodpdata01.sky.local@SKY.LOCAL", "sqoop", "atlas%%");
    }

    public static void main(String[] args) throws SQLException, ClassNotFoundException, IOException {

        String query = args[0];
        String host = "jdbc:hive2://mitstatlodpdata01.sky.local:10000/default;"
                + "transportMode=http;httpPath=cliservice;principal=hive/mitstatlodpdata01.sky.local@SKY.LOCAL";
        HiveConnector.getInstance(null).connect(host, "sqoop", "atlas%%");

        // Executing the given query
        System.out.println("Running: " + query);
        ResultSet res = HiveConnector.getInstance(null).executeQuery(query);
        while (res.next()) {
            System.out.println(String.valueOf(res.getString(1)) + "\t"+ res.getString(2));
        }
    }
}

