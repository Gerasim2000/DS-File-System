import java.io.*;
import java.net.*;
import java.util.*;



/*
TODO:


-----------------------------STORE---------------------------


CLIENT -> Dstore "Store filename filesize"
Dstore -> Client ACK
Client -> Dstore file_content
Dstore -> Controller "Store_ACK filename"


---------------------------------------------------------------


-----------------------------LOAD---------------------------


Client -> Dstore: "LOAD_DATA filename"
Dstore -> Client: file_content


--------------------------------------------------------------

-----------------------------REMOVE---------------------------


For each Dstore i storing filename 
Controller->Dstore i "REMOVE filename"

Once Dstore i finishes removing the file,
Dstore i -> Controller "REMOVE_ACK filename"


--------------------------------------------------------------


*/



public class Dstore {

    static boolean connected = false;
    public static void main(String[] args) throws Exception {
       
        int port = Integer.parseInt(args[0]);
        int cport = Integer.parseInt(args[1]);
        int timeout = Integer.parseInt(args[2]);
        String path = args[3];
        
        HashMap<Integer, List<String>> storeMap;


        File directory = new File(path);
        if (!directory.exists())
            if (!directory.mkdir())
                throw new RuntimeException("Error creating folder" + directory.getAbsolutePath());

                for(File fileInDirectory : directory.listFiles()) {
                    fileInDirectory.delete();
                }
        



        //"127.0.0.1"

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
                }

                InputStream in = controllerSocket.getInputStream();
                


                for(;;) {
                
                String line = inputFromController.readLine();

                if(line == null) {
                    inputFromController.close();
                    outputToController.close();
                    break;
                }
                
                String[] lineArgs = line.split(" ");
                String command=lineArgs[0];
                System.out.println("command "+command);


                // outputs a list of all files in the directory of the Dstore
                if(command.equals(Protocol.LIST_TOKEN)) {
                
                    outputToController.println(Protocol.LIST_TOKEN + " " + String.join(" ", directory.list()));
                    in.close();
                }

                //====================REMOVE=======================
                if(command.equals(Protocol.REMOVE_TOKEN)) {

                    String filename = lineArgs[1];
                    File file = new File(path + File.separator + filename);
                    if(file.delete()) outputToController.println(Protocol.REMOVE_ACK_TOKEN + " " + filename);
                    
                }

                
            }

            } catch(Exception e){
                System.out.println("error "+e);}
            }
        }catch(Exception e){
            System.out.println("error "+e);}


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
                
            String[] lineArgs = line.split(" ");
            String command=lineArgs[0];
            System.out.println("command "+command);



            //====================STORE=======================
            if(command.equals(Protocol.STORE_TOKEN)) {
                System.out.println("Entered STORE");
                String filename = lineArgs[1];
                Integer filesize = Integer.parseInt(lineArgs[2]);
                System.out.println("File: " + filename + "size: " + filesize);
                outToClient.println(Protocol.ACK_TOKEN);
                System.out.println("ENtered file");
                File file = new File(path + File.separator + filename);
                FileOutputStream outFile = new FileOutputStream(file);
                
                byte[] bytes = inStream.readNBytes(filesize);
                System.out.println("Finished reading");
                System.out.println(bytes);
                outFile.write(bytes);
                System.out.println("Finished store");
                outputToController.println(Protocol.STORE_ACK_TOKEN + " " + filename);
            }
            


            //=======================LOAD=======================
            if(command.equals(Protocol.LOAD_DATA_TOKEN)) {

                String filename = lineArgs[1];
                File file = new File(path + File.separator + filename);
                FileInputStream f = new FileInputStream(file);
                if(file.exists()) {
                    Integer filesize = (int) file.length();
                    out.write(f.readNBytes(filesize));
                    f.close();
                }
            }


            
            }

        } catch(Exception e) {}
        }).start();


    }
}
