/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package waypointsbeetrack;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Calendar;
import java.util.Date;

/**
 *
 * @author Tobal
 */
public class Log {
    public static void log(String mensaje) {
        Calendar c = Calendar.getInstance();
        c.setTime(new Date(System.currentTimeMillis()));
        int mes = c.get(Calendar.MONTH)+1;
        try {
            PrintWriter escribir = new PrintWriter(new FileWriter("logWaypointsBeetrack"+c.get(Calendar.YEAR)+mes+c.get(Calendar.DAY_OF_MONTH)+".txt",true));
            escribir.println(new Date(System.currentTimeMillis())+" - "+mensaje);
            escribir.close();
        }
        catch (IOException i) {
            i.printStackTrace();
        }
    }
}
