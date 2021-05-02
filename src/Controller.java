import java.io.*;
import java.net.*;
import java.util.List;

public class Controller {

    static int cport;
    List<Dstore> stores;
    static int timeout;
    static int rebalance_period;
    ServerSocket ss;

    public static void main(String[] args) throws IOException {

        cport = Integer.parseInt(args[0]);


        try{ServerSocket ss = new ServerSocket(4322);
            for(;;){
            try{
                System.out.println("waiting for connection");
                Socket client = ss.accept();
                System.out.println("connected");
                InputStream in = client.getInputStream();
                byte[] buf = new byte[1000]; int buflen;
                buflen = in.read(buf);

                String firstBuffer=new String(buf,0,buflen);
                int firstSpace=firstBuffer.indexOf(" ");
                String command=firstBuffer.substring(0,firstSpace);
                System.out.println("command "+command);


                if(command.equals("LIST\r\n")) {
                    int secondSpace=firstBuffer.indexOf(" ",firstSpace+1);
                    String fileName=firstBuffer.substring(firstSpace+1,secondSpace);
                    System.out.println("fileName "+fileName);
                    File outputFile = new File(fileName);
                    FileOutputStream out = new FileOutputStream(outputFile);
                    out.write(buf,secondSpace+1,buflen-secondSpace-1);
                    
                    while ((buflen=in.read(buf)) != -1){
                        System.out.print("*");
                        out.write(buf,0,buflen);
                    }

                    in.close();
                    client.close();
                    out.close();
                }


                else if(command.equals("get")){
                    int secondSpace=firstBuffer.indexOf(" ",firstSpace+1);
                    String fileName=firstBuffer.substring(firstSpace+1,secondSpace);
                    System.out.println("fileName "+fileName);
                    File inputFile = new File(fileName);
                    FileInputStream inf = new FileInputStream(inputFile);
                    OutputStream out = client.getOutputStream();
                    while ((buflen=inf.read(buf)) != -1){System.out.print("*");
                    out.write(buf,0,buflen);}in.close();
                    inf.close(); client.close(); out.close();
                } else System.out.println("unrecognised command");
            } catch(Exception e){
                System.out.println("error "+e);}}
        }catch(Exception e){
            System.out.println("error "+e);}
        System.out.println();
    }


//  ServerSocket ss = new ServerSocket(4322);
}
