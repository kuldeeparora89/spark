import kd.first.HelloWorld;
import kd.log.LogAnalyserApp;

/**
 * Created by kuldeep pc on 31-Mar-17.
 */
public class SparkMain {

    public static void main(String[] args) {




        String application=args[0];
        String master = args[1];




        if("first".equals(application)){
            new HelloWorld().countListValues(master);
        }
        else if("log".equals(application)){
            String logFile =args[2];
            new LogAnalyserApp().start(master,logFile);
        }








    }
}
