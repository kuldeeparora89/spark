import kd.first.HelloWorld;

/**
 * Created by kuldeep pc on 31-Mar-17.
 */
public class SparkMain {

    public static void main(String[] args) {




        String master = args[1];
        String application=args[0];



        if("first".equals(application)){
            new HelloWorld().countListValues(master);
        }








    }
}
