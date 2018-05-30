package dbconnection;

public class HiveConnectionProperties {
    String hive_connection_string  = "";
    String hive_username  = "";
    String hive_password  = "";
    String cb_bucket  = "";
    String cb_url  = "";
    String cb_bucket_password  = "";
    String cb_timeout   = "";
    String hive_query="";
    String hive_ktab_user = "";
    String hive_ktab_path = "";
    public HiveConnectionProperties(String hive_connection_string, String hive_username, String hive_password,
                                    String cb_bucket, String cb_url, String cb_bucket_password, String cb_timeoput,
                                    String hive_query, String hive_ktab_user, String hive_ktab_path) {
        super();
        this.hive_connection_string = hive_connection_string;
        this.hive_username = hive_username;
        this.hive_password = hive_password;
        this.cb_bucket = cb_bucket;
        this.cb_url = cb_url;
        this.cb_bucket_password = cb_bucket_password;
        this.cb_timeout = cb_timeoput;
        this.hive_query = hive_query;
        this.hive_ktab_user = hive_ktab_user;
        this.hive_ktab_path = hive_ktab_path;
    }
    public String getHive_connection_string() {
        return hive_connection_string;
    }
    public void setHive_connection_string(String hive_connection_string) {
        this.hive_connection_string = hive_connection_string;
    }
    public String getHive_username() {
        return hive_username;
    }
    public void setHive_username(String hive_username) {
        this.hive_username = hive_username;
    }
    public String getHive_password() {
        return hive_password;
    }
    public void setHive_password(String hive_password) {
        this.hive_password = hive_password;
    }
    public String getCb_bucket() {
        return cb_bucket;
    }
    public void setCb_bucket(String cb_bucket) {
        this.cb_bucket = cb_bucket;
    }
    public String getCb_url() {
        return cb_url;
    }
    public void setCb_url(String cb_url) {
        this.cb_url = cb_url;
    }

    public String getCb_timeout() {
        return cb_timeout;
    }
    public void setCb_timeout(String cb_timeout) {
        this.cb_timeout = cb_timeout;
    }
    public String getHive_query() {
        return hive_query;
    }
    public void setHive_query(String hive_query) {
        this.hive_query = hive_query;
    }
    public String getHive_ktab_user() {
        return hive_ktab_user;
    }
    public void setHive_ktab_user(String hive_ktab_user) {
        this.hive_ktab_user = hive_ktab_user;
    }
    public String getHive_ktab_path() {
        return hive_ktab_path;
    }
    public void setHive_ktab_path(String hive_ktab_path) {
        this.hive_ktab_path = hive_ktab_path;
    }
    public String getCb_bucket_password() {
        return cb_bucket_password;
    }
    public void setCb_bucket_password(String cb_bucket_password) {
        this.cb_bucket_password = cb_bucket_password;
    }
    @Override
    public String toString() {
        return "HiveCouchBaseExporterPref [hive_connection_string=" + hive_connection_string + ", hive_username="
                + hive_username + ", hive_password=" + hive_password + ", cb_bucket=" + cb_bucket + ", cb_url=" + cb_url
                + ", cb_bucket_password=" + cb_bucket_password + ", cb_timeoput=" + cb_timeout + ", hive_query="
                + hive_query + ", hive_ktab_user=" + hive_ktab_user + ", hive_ktab_path=" + hive_ktab_path + "]";
    }

}
