// Requires JDK 1.8 
// Run it with:
// javac Main.java && java -cp .:dremio-jdbc-driver-4.2.1-202004111451200819-0c3ecaea.jar Main

// javac Main.java
import java.sql.*;
import java.util.Properties;

public class Main {

    public static void main(String[] args) {
        final String DB_URL = "jdbc:dremio:direct=localhost:31010;";
        final String USER = "dremio";
        final String PASS = "dremio123";
        Properties props = new Properties();
        props.setProperty("user",USER);
        props.setProperty("password",PASS);

        Connection conn = null;
        Statement stmt = null;
        try {

            System.out.println("Connecting to database...");
            conn = DriverManager.getConnection(DB_URL, props);

            System.out.println("Creating statement...");
            stmt = conn.createStatement();
            String sql;
            sql = "SELECT user";
            ResultSet rs = stmt.executeQuery(sql);

            while (rs.next()) {
                System.out.print(rs.getString("user"));

            }
            rs.close();
            stmt.close();
            conn.close();
        } catch (SQLException se) {
            se.printStackTrace();
        }
    }
}