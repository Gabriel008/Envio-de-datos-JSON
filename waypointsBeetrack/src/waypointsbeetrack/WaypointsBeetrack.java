/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package waypointsbeetrack;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URL;
import java.net.URLEncoder;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import javax.net.ssl.HttpsURLConnection;
import org.json.*;

/**
 *
 * @author Gabriel Valenzuela
 */
public class WaypointsBeetrack {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws Exception {
        // TODO code application logic here
        
        String url;
        
        Properties prop = new Properties();
        InputStreamReader input = null;
        input = new FileReader("config.properties");
        prop.load(input);
        Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        Connection conn = null;
        Statement st;
        ResultSet rs;
        String cadena = "";
        String eid="";
        String device_id="";
        String edt="";
        String carnumber=""; 
        String latitude="";
        String longitude="";
        String message_id = "";
        
        while (true) {
            try {
                //db_connect_string = jdbc:sqlserver://150.0.20.202;databaseName=DB_GPS
                //db_connect_string = jdbc:sqlserver://;servername=localhost\\SQLEXPRESS;databaseName=DB_GPS
                conn = DriverManager.getConnection(prop.getProperty("db_connect_string"), prop.getProperty("db_user"), prop.getProperty("db_pass"));
            }
            catch (SQLException e) {
                e.printStackTrace();
                Log.log(e.toString());
                if (conn!=null)
                    conn.close();
                int c = 0;
                while (c==0) {
                    try {
                        conn = DriverManager.getConnection(prop.getProperty("db_connect_string"), prop.getProperty("db_user"), prop.getProperty("db_pass"));
                        c = 1;
                    }
                    catch(SQLException s) {
                        s.printStackTrace();
                        Log.log(s.toString());
                    }
                    Thread.sleep(15000);
                }
            }
            
            String imei,reg="";
            int cont=0;
            try {
                System.out.println("---------------------------------");
                    System.out.println("Inicio Proceso");
                    System.out.println("---------------------------------");
                    System.out.println("---------------------------------");
                    st=conn.createStatement();                    
                    String SQuery1="select * from VehiculosBeetrackAPI";
                    System.out.println(SQuery1);
                    rs=st.executeQuery(SQuery1);
                    
                    
                    while (rs.next()){
                    
                    imei=rs.getObject(1).toString();
                    reg=rs.getObject(3).toString();
                
                    System.out.println("---------------------------------");
                    System.out.println(imei+","+reg);
                    System.out.println("---------------------------------");
                
                st = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
                
                /*String sql = "select 'X' from INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME='KarmaWS'";
                rs = st.executeQuery(sql);
                if (rs.getRow()==0) {
                    st = conn.createStatement();
                    st.execute("CREATE TABLE KarmaWS (REG BIGINT IDENTITY(1,1) PRIMARY KEY, DATA NVARCHAR(1024), ESTADO INT)");
                }*/
         
              
                
                url = prop.getProperty("url");
                URL obj = new URL(url);                
                String[] fields = prop.getProperty("location_fields").split(",");
                HttpsURLConnection con = (HttpsURLConnection)obj.openConnection();
                con.setRequestMethod("POST");
                con.setRequestProperty("Content-Type", "application/json");
                String location_fields = "";
                for (int i=0;i<fields.length;i++) {
                    if (i==fields.length-1)
                        location_fields += "\""+fields[i]+"\"";
                    else
                        location_fields += "\""+fields[i]+"\",";
                }
                System.out.println(location_fields);
                //{"cmd":"events","fields":"eid,device_id,edt,carnumber,extra,latitude,longitude,message_id,car_id,extra_base64,odo,head,alt","device":"356306054837407"}
                //String json = "[{\"cmd\":\"login\",\"user\": \""+prop.getProperty("user")+"\",\"password\":\""+prop.getProperty("pass")+"\"},{\"cmd\":\"location\",\"fields\":["+location_fields+"]}]";
               
                String json = "[{\"cmd\":\"login\",\"user\": \""+prop.getProperty("user")+"\",\"password\":\""+prop.getProperty("pass")+"\"},{\"cmd\":\"events\",\"from_id\":\""+reg+"\",\"fields\":["+location_fields+"],\"device\":\""+imei+"\"}]";
                 System.out.println(json);
                con.setDoOutput(true);
                con.setDoInput(true);
                DataOutputStream wr = new DataOutputStream(con.getOutputStream());
                wr.writeBytes(json);
                wr.flush();
                wr.close();
                BufferedReader in = new BufferedReader(
                            new InputStreamReader(con.getInputStream()));
                    String inputLine;
                    StringBuffer response = new StringBuffer();

                    while ((inputLine = in.readLine()) != null) {
                            response.append(inputLine);
                    }
                    in.close();
                    //print result
                    //JSONObject jo = new JSONObject(response.toString());
                    System.out.println(response.toString());
                    Log.log(response.toString());
                    
                    
                    JSONArray ja = new JSONArray(response.toString());
                    JSONObject jo = new JSONObject(ja.get(1).toString());
                    JSONArray data = new JSONArray(jo.get("data").toString());
                    for (int i=0;i<data.length();i++) {
                         cont++;
                         System.out.println("---------------------------------");
                         System.out.println("Registro"+cont);
                         System.out.println("---------------------------------");
                        System.out.println(data.get(i).toString());
                        JSONObject veh_data = new JSONObject(data.get(i).toString());
                        for (int j=0;j<fields.length;j++) {
                            try {
                               //eid,device_id,edt,carnumber,latitude,longitude,message_id
                                if (fields[j].equals("eid")){
                                eid=veh_data.getString(fields[j]);
                                }
                                if (fields[j].equals("device_id")){
                                device_id=imei;
                                }
                                if (fields[j].equals("edt")){
                                edt=veh_data.getString(fields[j]);
                                }
                                if (fields[j].equals("carnumber")){
                                carnumber=veh_data.getString(fields[j]);
                                }
                                 if (fields[j].equals("latitude")){
                                latitude=veh_data.getString(fields[j]);
                                }
                                if (fields[j].equals("longitude")){
                                longitude=veh_data.getString(fields[j]);
                                }
                                
                                if (fields[j].equals("message_id")){
                                message_id=veh_data.getString(fields[j]);
                                }
                                System.out.println(fields[j]+": "+veh_data.getString(fields[j]));
                                if (j==0)
                                    cadena = veh_data.getString(fields[j]);
                                else
                                    cadena += ";" + veh_data.getString(fields[j]);
                            }
                            catch (JSONException je) {
                                if (j==1){
                                       System.out.println("---ENTRE: "+ imei);
                                    cadena += ";" + imei;
                                }
                                if (j!=1){
                                System.out.println("Campo "+fields[j]+" no existe");
                                cadena += ";";
                                }
                            }
                             
                        }

                        Log.log("cadena: "+cadena);
                        String cadena2="";
                        cadena2=carnumber+","+latitude+","+longitude+","+edt;
                        System.out.println("cadena1: "+cadena);
                        System.out.println("cadena2: "+cadena2);
                         System.out.println("--ID DEL VEHICULO--"+cont);
                         System.out.println("*"+fields[6]+": "+veh_data.getString(fields[6]));
                         System.out.println("---------------------------------");
                          Thread.sleep(Integer.parseInt(prop.getProperty("interval"))*1000);
                         
                        
//                        st = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
//                        System.out.println("SELECT * FROM KarmaWS where DATA='"+cadena+"'");
//                        rs = st.executeQuery("SELECT * FROM KarmaWS where DATA='"+cadena+"'");
//                        if (!rs.next()) 
//                        {
                            st = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
                            //st.executeUpdate("INSERT INTO KarmaWS (DATA,ESTADO) VALUES ('"+cadena+"',0)");
                           
                            PostJSON(carnumber,latitude,longitude,edt);
                           
                            st.executeUpdate("update VehiculosBeetrackAPI set codreg='"+veh_data.getString(fields[6])+"' where imei='"+imei+"'");
//                        }   
                    }
                    cont=0;
                    Thread.sleep(Integer.parseInt(prop.getProperty("interval1"))*1000);
                /*for (int i=0;i<fields.length;i++) {
                    System.out.println(fields[i]);
                }*/
                
                
             }
            Thread.sleep(Integer.parseInt(prop.getProperty("interval2"))*1000);
                
            }
            catch (Exception e) {
                e.printStackTrace();
                Log.log(e.toString());
            }
        }
        
        
    }
    
  public static void PostJSON(String Patente, String Lat,String Lon,String Fech) throws Exception 
{
    	String url = "https://app.beetrack.com/api/external/v1/gps/";
		URL obj = new URL(url);
		HttpsURLConnection con = (HttpsURLConnection) obj.openConnection();
 
		con.setRequestMethod("POST");
                con.setRequestProperty("Content-Type", "application/json");
		con.setRequestProperty("X-AUTH-TOKEN", "8f04f941357baad33d1be2919f1a9fc6bf1f3485967973cb03ff7314143d9c49");

 //{ "identifier" : "A4677", "waypoints" : [{ "latitude" : "-34.586949", "longitude" : "-70.9901182" ,"sent_at":""   }]}
                
 String json = "{ \"identifier\" : \""+Patente+"\", \"waypoints\" : [{ \"latitude\" : \""+Lat+"\", \"longitude\" : \""+Lon+"\" ,\"sent_at\":\""+Fech+"\"   }]}";
                 System.out.println(json);
                con.setDoOutput(true);
                con.setDoInput(true);
                DataOutputStream wr = new DataOutputStream(con.getOutputStream());
                wr.writeBytes(json);
                wr.flush();
                wr.close();
                BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
                    String inputLine;
                    StringBuffer response = new StringBuffer();

                    while ((inputLine = in.readLine()) != null) {
                            response.append(inputLine);
                    }
                    in.close();
                    //print result
                    //JSONObject jo = new JSONObject(response.toString());
                    System.out.println(response.toString());
                    Log.log(response.toString());
 
 
 
//		con.setDoOutput(true);
//		OutputStreamWriter outputStreamWriter = new OutputStreamWriter(con.getOutputStream());
  
//        outputStreamWriter.flush();
 
//		int responseCode = con.getResponseCode();
//		System.out.println("\nSending 'POST' request to URL : " + url);
//		//System.out.println("Post parameters : " + urlParameters);
//		System.out.println("Response Code : " + responseCode);
// 
//		BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
//		String inputLine;
//		StringBuffer response = new StringBuffer();
// 
//		while ((inputLine = in.readLine()) != null) {
//			response.append(inputLine);
//		}
//		in.close();
// 
//		System.out.println(response.toString());
}
     
}
