import java.io.*;
import java.net.*;
import java.util.*;





public class Dstore {

    static boolean connected = false;
    public static void main(String[] args) throws Exception {
       
        int port = Integer.parseInt(args[0]);
        int cport = Integer.parseInt(args[1]);
        int timeout = Integer.parseInt(args[2]);
        String path = args[3];
        


        File directory = new File(path);
        if (!directory.exists())
            if (!directory.mkdir())
                throw new RuntimeException("Error creating folder" + directory.getAbsolutePath());

                for(File fileInDirectory : directory.listFiles()) {
                    fileInDirectory.delete();
                }
        



        //logger
        DstoreLogger.init(Logger.LoggingType.ON_FILE_AND_TERMINAL, port);

        //controller connection
        Socket controllerSocket = new Socket(InetAddress.getByName("127.0.0.1"), cport);
        BufferedReader inputFromController = new BufferedReader(new InputStreamReader(controllerSocket.getInputStream()));
        PrintWriter outputToController = new PrintWriter(new OutputStreamWriter(controllerSocket.getOutputStream()),true);

        // CONNECTION WITH CONTROLLER
        new Thread(() -> {

        try{
            
            for(;;){
            try{


                if(!connected) {
                    connected = true;

                    outputToController.println(Protocol.JOIN_TOKEN + " " + port);
                    DstoreLogger.getInstance().messageSent(controllerSocket, new String(Protocol.JOIN_TOKEN + " " + port));
                }

                
                for(;;) {
                
                String line = inputFromController.readLine();
                DstoreLogger.getInstance().messageReceived(controllerSocket, line);
                if(line == null) {
                    break;
                }

                
               
                
                String[] lineArgs = line.split(" ");
                String command=lineArgs[0];
                System.out.println("command "+command);


                // outputs a list of all files in the directory of the Dstore
                if(command.equals(Protocol.LIST_TOKEN)) {
                
                    if(lineArgs.length != 1) {

                        System.err.println("Error, malformed message detected!");;
                        continue;
                    }
                    
                    if(directory.list() != null)  {

                        outputToController.println(Protocol.LIST_TOKEN + " " + String.join(" ", directory.list()));
                        DstoreLogger.getInstance().messageSent(controllerSocket, new String(Protocol.LIST_TOKEN + " " + String.join(" ", directory.list())));
                    }
                    else  {

                        outputToController.println(Protocol.LIST_TOKEN);
                        DstoreLogger.getInstance().messageSent(controllerSocket, Protocol.LIST_TOKEN);
                    }
                    
                }

                //====================REMOVE=======================
                if(command.equals(Protocol.REMOVE_TOKEN)) {

                    System.out.println("Entering remove");

                    if(lineArgs.length != 2) {

                        System.err.println("Error, malformed message detected!");;
                        continue;
                    }

                    String filename = lineArgs[1];
                    System.out.println(path+File.separator+ filename);
                    
                    File file = new File(path + File.separator + filename);
                    if(file.delete()) {
                        System.out.println("Delete complete... sending ack");

                        outputToController.println(Protocol.REMOVE_ACK_TOKEN + " " + filename);
                        DstoreLogger.getInstance().messageSent(controllerSocket, new String(Protocol.REMOVE_ACK_TOKEN + " " + filename));
                    }
                }

                
            }

            } catch(Exception e){
                System.out.println("error "+e);
                e.printStackTrace();}
            }
        }catch(Exception e){
            System.out.println("error "+e);
            e.printStackTrace();}


        }).start();






  
        // CONNECTION WITH CLIENTS
        new Thread(() -> {
            try {
            ServerSocket ss = new ServerSocket(port);                
            for(;;) {
            
            Socket client = ss.accept();

            PrintWriter outToClient = new PrintWriter(new OutputStreamWriter (client.getOutputStream()),true);
            BufferedReader inFromClient = new BufferedReader(new InputStreamReader (client.getInputStream()));
            InputStream inStream = client.getInputStream();
            OutputStream out = client.getOutputStream();

            String line = inFromClient.readLine();

            if(line == null) {
                inFromClient.close();
                outToClient.close();
                break;
            }

            
            DstoreLogger.getInstance().messageReceived(client, line);
                
            String[] lineArgs = line.split(" ");
            String command=lineArgs[0];
            System.out.println("command "+command);



            //====================STORE=======================
            if(command.equals(Protocol.STORE_TOKEN)) {
                
                System.out.println("Entered STORE");

                if(lineArgs.length != 3) {

                    System.err.println("Error, malformed message detected!");;
                    continue;
                }

                String filename = lineArgs[1];
                Integer filesize = Integer.parseInt(lineArgs[2]);
                System.out.println("File: " + filename + " size: " + filesize);


                outToClient.println(Protocol.ACK_TOKEN);
                DstoreLogger.getInstance().messageSent(client, Protocol.ACK_TOKEN);


                System.out.println("ENtered file");
                File file = new File(path + File.separator + filename);
                FileOutputStream outFile = new FileOutputStream(file);
                
                byte[] bytes = inStream.readNBytes(filesize);
                System.out.println("Finished reading");
                
                outFile.write(bytes);
                System.out.println("Finished store");


                outputToController.println(Protocol.STORE_ACK_TOKEN + " " + filename);
                DstoreLogger.getInstance().messageSent(controllerSocket, new String(Protocol.STORE_ACK_TOKEN + " " + filename));

                outFile.close();
                client.close();
            }
            


            //=======================LOAD=======================
            if(command.equals(Protocol.LOAD_DATA_TOKEN)) {

                if(lineArgs.length != 2) {

                    System.err.println("Error, malformed message detected!");;
                    continue;
                }

                String filename = lineArgs[1];
                File file = new File(path + File.separator + filename);
                
            
                if(file.exists()) {
                    Integer filesize = (int) file.length();
                    FileInputStream f = new FileInputStream(file);
                    out.write(f.readNBytes(filesize));
                    out.flush();
                    f.close();
                }
                
                client.close();
            }


            
            }

        } catch(Exception e) {
            e.printStackTrace();
        }
        }).start();


    }
}
