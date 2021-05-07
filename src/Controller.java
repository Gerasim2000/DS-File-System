import java.io.*;
import java.net.*;
import java.security.KeyStore.Entry;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;





/*
TODO:


RELOAD 

Error handling

Index states

Rebalance operation

-----------------------------REMOVE---------------------------

Client -> Controller "REMOVE filename"

For each Dstore i storing filename 
Controller->Dstore i "REMOVE filename"

Once Dstore i finishes removing the file,
Dstore i -> Controller "REMOVE_ACK filename"

Controller -> Client:REMOVE_COMPLETE

--------------------------------------------------------------


*/











public class Controller {

    String index = "";

    // port -> list of all files on the port
    static ConcurrentHashMap<Integer, ArrayList<String>> portToFiles = new ConcurrentHashMap<>();

    // filename -> all ports that store it
    static ConcurrentHashMap<String, ArrayList<Integer>> fileToPorts = new ConcurrentHashMap<>();

    // filename -> all ports that store it
    static ConcurrentHashMap<String, Integer> fileToSize = new ConcurrentHashMap<>();

    // filename -> acknowledgement
    static ConcurrentHashMap<String, Integer> fileToRemainingACKs = new ConcurrentHashMap<>();

    // current ports for storing
    static Vector<String> selectedPorts = new Vector<>();

    static AtomicBoolean storeComplete = new AtomicBoolean(false);

    static Integer countDstores = 0;

    public static void main(String[] args) throws IOException {


        Integer cport = Integer.parseInt(args[0]);
        Integer R = Integer.parseInt(args[1]);
        Integer timeout = Integer.parseInt(args[2]);
        Integer rebalance_period = Integer.parseInt(args[3]);

        try{
            ServerSocket ss = new ServerSocket(cport);

            
            for(;;){
                System.out.println("Waiting for connection");
                Socket client = ss.accept();
                System.out.println("connected");
            new Thread(() -> {
            try{ 

                for (;;) {
                System.out.println("Entering loop");
                boolean isClient = true;

                BufferedReader inputFromClient = new BufferedReader(new InputStreamReader(client.getInputStream()));
                PrintWriter outputToClient = new PrintWriter(new OutputStreamWriter(client.getOutputStream()), true);
                
                String line = inputFromClient.readLine();

                if(line == null) {
                    client.close();
                    break;
                }

                String[] lineArgs = line.split(" ");
                System.out.println("test2");
                String command;
                if (lineArgs[0].equals(null)) command = line;
                else command=lineArgs[0];
                

                System.out.println("command "+command);



                // ----------------------------STORE--------------------------------------------
                
                if(command.equals(Protocol.STORE_TOKEN)){
                    System.out.println("ENTERING STORE OPERATION");

                    String filename = lineArgs[1];
                    Integer filesize = Integer.parseInt(lineArgs[2]);

                    if(fileToPorts.keySet().contains(filename)) {
                        System.out.println("File already exists");
                        outputToClient.println(Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);
                        continue;
                    }


                    System.out.println("name " + filename + " size: " + filesize);
                    selectedPorts = getRPorts();
                    System.out.println(selectedPorts);
                    fileToRemainingACKs.put(filename, R);
                    
                    
                    if(selectedPorts.size() < R) {
                        outputToClient.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                        continue;
                    }
                    
                    // adds all the ports to the file
                    ArrayList<Integer> currentPorts = new ArrayList<>();
                    if(fileToPorts.get(filename) != null) currentPorts = fileToPorts.get(filename);
                    

                    
                    // Adds all of the files to the ports where they belong
                    for(Integer port : getPortsInt(selectedPorts)) {
                        
                        ArrayList<String> currentFiles = portToFiles.get(port);
                        currentFiles.add(filename);
                        portToFiles.put(port, currentFiles);
                    }

                    outputToClient.println(Protocol.STORE_TO_TOKEN + " " + String.join(" ", selectedPorts));

                    long startTime = System.currentTimeMillis();
                    System.out.println("Infinite loop");
                    while(false||(System.currentTimeMillis()-startTime) < timeout) {
                        if(storeComplete.get()) {
                            System.out.println("REDII");
                            outputToClient.println(Protocol.STORE_COMPLETE_TOKEN);

                            //Update the values in the hashmaps
                            currentPorts.addAll(getPortsInt(selectedPorts));
                            fileToPorts.put(filename,currentPorts);
                            fileToSize.put(filename, filesize);
                            storeComplete.set(false);
                            break;
                        } 
                    }
                    System.out.println("Out of loop");
                    if(!storeComplete.get());

                } 
                


                else if (command.equals(Protocol.STORE_ACK_TOKEN)) {

                    String filename = lineArgs[1];
                    System.out.println("Received store ack " + filename + " ");

                    Integer remaining = fileToRemainingACKs.get(filename);
                    fileToRemainingACKs.put(filename, remaining - 1);
                    if (fileToRemainingACKs.get(filename) <= 0) storeComplete.set(true);;
                    System.out.println("Store completed for port");
                }


                // ----------------------------LOAD--------------------------------------------
                else if (command.equals(Protocol.LOAD_TOKEN)) {
                    System.out.println("Entering Load");
                    String filename = lineArgs[1];
                    Integer port = fileToPorts.get(filename).get(0);
                    System.out.println("Selected port: " + port);
                    outputToClient.println(Protocol.LOAD_FROM_TOKEN + " " + port + " " + fileToSize.get(filename));
                }


                // ----------------------------LIST--------------------------------------------
                else if(command.equals(Protocol.LIST_TOKEN)) {
                
                    outputToClient.println(Protocol.LIST_TOKEN + " " + String.join(" ", fileToSize.keySet()));
                }


                // ----------------------------JOIN--------------------------------------------
                else if (command.equals(Protocol.JOIN_TOKEN)) {
                    Integer port = Integer.parseInt(lineArgs[1]);
                    portToFiles.put(port, new ArrayList<>());
                    countDstores++;
                }

                // ----------------------------REMOVE--------------------------------------------
                else if (command.equals(Protocol.REMOVE_TOKEN)) {
                    
                    System.out.println("Entering Remove");
                    String filename = lineArgs[1];
 
                    // Vector<String> for files which need to be deleted

                }
            }
            
            } catch(Exception e){
                System.out.println("error "+e);}
            }).start();
            }
        }catch(Exception e){
            System.out.println("error "+e);}
        System.out.println();
    }


    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    //---------------------------Function to select R ports-----------------------------------------------
    
    public static Vector<String> getRPorts() {

        Vector<String> stringPorts = new Vector<>();
        List<Map.Entry<String, Integer> > list = new LinkedList<Map.Entry<String, Integer> >(getFileToSize().entrySet());
 
        // Sort the list
        Collections.sort(list, new Comparator<Map.Entry<String, Integer> >() {
            public int compare(Map.Entry<String, Integer> o1,
                               Map.Entry<String, Integer> o2)
            {
                return (o1.getValue()).compareTo(o2.getValue());
            }
        });
         
        // put data from sorted list to hashmap
        HashMap<String, Integer> temp = new LinkedHashMap<String, Integer>();
        for (Map.Entry<String, Integer> aa : list) {
            temp.put(aa.getKey(), aa.getValue());
        }
        
        for (Map.Entry<String, Integer> en : temp.entrySet()) {
            stringPorts.add(en.getKey());
        }

        return stringPorts;
    }

    public static HashMap<String,Integer> getFileToSize () {
        HashMap<String,Integer> portToSize = new HashMap<>();
        for (Integer port  : portToFiles.keySet()) {

            portToSize.put(port.toString(), portToFiles.get(port).size());
        }
        return portToSize;
    }


    // Returns the list of ports as Integer
    public static ArrayList<Integer> getPortsInt (Vector<String> portString) {
        ArrayList<Integer> portsInt = new ArrayList<>();
        for (int i = 0; i < portString.size(); i++) {
            portsInt.add(Integer.parseInt(portString.get(i)));
        }
        return portsInt;
    }

}
