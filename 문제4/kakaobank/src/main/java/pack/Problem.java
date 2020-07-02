package pack;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import java.util.ArrayList;
import java.util.List;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import au.com.bytecode.opencsv.CSVWriter;


public class Problem {

    static String result = null;
    static Connection conn = null;
    static Statement stmt = null;
    static PreparedStatement pstmt = null;

    public static void main(String[] argv) {

        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            return;
        }

        try {
            conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/kakaobank?serverTimezone=UTC", "root", "1234");
        } catch (SQLException e) {
            e.printStackTrace();
            return;
        }

        try {
            stmt = conn.createStatement();
            String sql =  "DROP TABLE IF EXISTS TEMP_RESULT";
            stmt.executeUpdate(sql);
            sql = "CREATE TABLE IF NOT EXISTS TEMP_RESULT(";
            sql += " MENU_ROUTE VARCHAR(200),";
            sql += " TAKE_TIME BIGINT ";
            sql += " )";
            stmt.executeUpdate(sql);


            List<Object> result_list = new ArrayList<Object>();

            sql = " SELECT ";
            sql += " ORDERING.MENU_NM";
            sql += ", case when ORDERING.MENU_NM != 'logout' then TIMESTAMPDIFF(SECOND, DATE_FORMAT(ORDERING.LOG_TKTM, '%Y-%m-%d %H:%i:%S'), DATE_FORMAT(AFTER_ORDERING.LOG_TKTM, '%Y-%m-%d %H:%i:%S')) else 0 end AS STAY_TIME";
            sql += " FROM";
            sql += " (SELECT *, ROW_NUMBER() OVER (PARTITION BY USR_NO ORDER BY LOG_TKTM) AS ROW_NUM FROM MENU_LOG) AS ORDERING";
            sql += " LEFT OUTER JOIN";
            sql += " ( SELECT *, ROW_NUMBER() OVER (PARTITION BY USR_NO ORDER BY LOG_TKTM)  AS ROW_NUM FROM MENU_LOG) AS AFTER_ORDERING";
            sql += " ON ORDERING.USR_NO = AFTER_ORDERING.USR_NO AND ORDERING.ROW_NUM = AFTER_ORDERING.ROW_NUM-1";
            sql += " ORDER BY ORDERING.USR_NO, ORDERING.LOG_TKTM";


            pstmt = conn.prepareStatement(sql);
            ResultSet rs = pstmt.executeQuery();

            List<Object> temp_list = new ArrayList<Object>();
            String menu ="";
            int time = 0;

            while (rs.next()){

                String MENU_NM = rs.getString("MENU_NM");
                String STAY_TIME = rs.getString("STAY_TIME");


                if (MENU_NM.equals("logout")){
                    menu+= MENU_NM;
//                    temp_list.add(menu);
//                    temp_list.add(time);
                    sql = "INSERT INTO TEMP_RESULT VALUES ('"+ menu + "' , " + time + " ) ;";
                    menu = "";
                    time = 0;
//                    result_list.add(temp_list);
                    pstmt = conn.prepareStatement(sql);
                    pstmt.executeUpdate();
//                    temp_list = new ArrayList<Object>();
                }
                else{
                    menu+= MENU_NM+'-';
                    time+= Integer.parseInt(STAY_TIME);
                }
            }

            sql = "SELECT MENU_ROUTE , MAX(TAKE_TIME) AS MAX_TAKE_TIME , MIN(TAKE_TIME) AS  MIN_TAKE_TIME";
            sql += " FROM TEMP_RESULT ";
            sql += " GROUP BY MENU_ROUTE";
            sql += " ORDER BY COUNT(*) desc, MAX(TAKE_TIME) DESC LIMIT 3";

            pstmt = conn.prepareStatement(sql);

            CSVWriter writer = new CSVWriter(new FileWriter("output.csv"), ',');

            boolean includeHeaders = true;

            ResultSet myResultSet = pstmt.executeQuery();

            writer.writeAll(myResultSet, includeHeaders);

            writer.close();


        } catch (SQLException | IOException e) {
            e.printStackTrace();
        } finally {
            try {
                pstmt.close();
                stmt.close();
                conn.close();
            } catch (SQLException e){
                e.printStackTrace();
            }
        }

    }
}
