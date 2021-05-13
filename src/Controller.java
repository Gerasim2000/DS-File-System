import java.io.*;
import java.net.*;
import java.security.KeyStore.Entry;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;





/*
TODO:


RELOAD 

Error handling

Index states

Rebalance operation



*/











public class Controller {

    // port -> list of all files on the port
    static ConcurrentHashMap<Integer, ArrayList<String>> portToFiles = new ConcurrentHashMap<>();

    // filename -> all ports that store it
    static ConcurrentHashMap<String, ArrayList<Integer>> fileToPorts = new ConcurrentHashMap<>();

    // filename -> all ports that store it
    static ConcurrentHashMap<String, Integer> fileToSize = new ConcurrentHashMap<>();

    // filename -> acknowledgement
    static ConcurrentHashMap<String, Integer> fileToRemainingStoreACKs = new ConcurrentHashMap<>();

    // filename -> acknowledgement
    static ConcurrentHashMap<String, Integer> fileToRemainingRemoveACKs = new ConcurrentHashMap<>();

    // filename -> acknowledgement
    static ConcurrentHashMap<Integer, Socket> portToSocket = new ConcurrentHashMap<>();

    // current ports for storing
    static Vector<String> selectedPorts = new Vector<>();

    // stores elements that need to be stored
    static Vector<String> storeQueue = new Vector<>();

    // stores elements that need to be removed
    static Vector<String> removeQueue = new Vector<>();

    // stores elements that need to be removed
    static Vector<String> loadQueue = new Vector<>();
    
    // filename -> index for loading (from portToSocket)
    static ConcurrentHashMap<String, Integer> fileToIndex = new ConcurrentHashMap<>();

    static Integer countDstores = 0;

    public void initializeController(Integer cport, Integer R, Integer timeout, Integer rebalance_period) {


        try{
            ServerSocket ss = new ServerSocket(cport);

            //logger
            ControllerLogger.init(Logger.LoggingType.ON_FILE_AND_TERMINAL); 
            
            for(;;){
                System.out.println("Waiting for connection");
                Socket client = ss.accept();
                System.out.println("connected");
            new Thread(() -> {
            try{ 

                BufferedReader inputFromClient = new BufferedReader(new InputStreamReader(client.getInputStream()));
                PrintWriter outputToClient = new PrintWriter(new OutputStreamWriter(client.getOutputStream()), true);
                Integer portIndex = 0;
                String line = null;
                Boolean isClient = true;
                for (;;) {

                System.out.println("Entering loop");
                if(client.isClosed()) {
                    System.out.println("YO chetesh kato e close-nat kopele!");
                    break;
                }

                line = inputFromClient.readLine();
                ControllerLogger.getInstance().messageReceived(client, line);


                if(line == null) {
                    client.close();
                    System.out.println("THREAD CLOSED");
                    if(!isClient)  {
                        countDstores--;
                        
                    }
                    break;
                }

                String[] lineArgs = line.split(" ");
                String command = lineArgs[0];
                System.out.println("command "+command + ".");

                
                // ----------------------------JOIN--------------------------------------------
                if (command.equals(Protocol.JOIN_TOKEN)) {
                    
                    if(lineArgs.length != 2) {

                        System.err.println("Error, malformed message detected!");;
                        continue;
                    }
                    
                    Integer port = Integer.parseInt(lineArgs[1]);

                    //log
                    ControllerLogger.getInstance().dstoreJoined(client, port);  

                    isClient = false;
                    portToFiles.put(port, new ArrayList<>());
                    portToSocket.put(port, client);
                    countDstores++;
                }



                // ----------------------------STORE--------------------------------------------
                
                if(command.equals(Protocol.STORE_TOKEN)){



                //  checks if there are enough dstores
                if(countDstores < R) {

                    outputToClient.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                    ControllerLogger.getInstance().messageSent(client, Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);

                    continue;
                }
                    
                    if(lineArgs.length != 3) {

                        System.err.println("Error, malformed message detected!");;
                        continue;
                    }
                    
                    System.out.println("STORE IN PROGRESS");
                    String filename = lineArgs[1];
                    Integer filesize = Integer.parseInt(lineArgs[2]);


                    if(fileToSize.keySet().contains(filename)) {
                        System.out.println("File already exists");

                        //Log error file exists
                        
                        outputToClient.println(Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);
                        ControllerLogger.getInstance().messageSent(client, Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);

                        continue;
                    }
                    
                    // ----------------------------- STORE INDEX UPDATE
                    synchronized(this) {

                        if(!storeQueue.contains(filename)) storeQueue.add(filename);
                        else  {

                            //Log error file exists
                            outputToClient.println(Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);
                            ControllerLogger.getInstance().messageSent(client, Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);

                            continue;
                        }
                    }
                    
                    
                    




                    System.out.println("name " + filename + " size: " + filesize);
                    selectedPorts = getRPorts();
                    System.out.println(selectedPorts);
                    fileToRemainingStoreACKs.put(filename, R);
                    
                    
                    if(selectedPorts.size() < R) {

                        
                        outputToClient.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                        //Log error Not enough dstores
                        ControllerLogger.getInstance().messageSent(client, Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);

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

                    String msg = Protocol.STORE_TO_TOKEN + " " + String.join(" ", selectedPorts);

                    
                    outputToClient.println(msg);

                    //Log store_to_TOKEn
                    ControllerLogger.getInstance().messageSent(client, msg);



                    long startTime = System.currentTimeMillis();
                    System.out.println("Infinite loop");
                    while(false||(System.currentTimeMillis()-startTime) < timeout) {
                        if(fileToRemainingStoreACKs.get(filename) <= 0) {
                            System.out.println("All store acks received");

                            //Log error file exists
                            
                            outputToClient.println(Protocol.STORE_COMPLETE_TOKEN);
                            ControllerLogger.getInstance().messageSent(client, Protocol.STORE_COMPLETE_TOKEN);

                            //UPDATE VALUES + INDEX
                            currentPorts.addAll(getPortsInt(selectedPorts));
                            fileToPorts.put(filename,currentPorts);
                            fileToIndex.put(filename, 0);
                            fileToSize.put(filename, filesize);

                            synchronized(this) {
                                storeQueue.remove(filename);
                            }
                            
                            break;
                        } 
                    }
                    System.out.println("Out of loop");

                } 
                

                // --------------------------STORE_ACK_----------------------------------
                if (command.equals(Protocol.STORE_ACK_TOKEN)) {

                    if(lineArgs.length != 2) {

                        System.err.println("Error, malformed message detected!");;
                        continue;
                    }

                    String filename = lineArgs[1];
                    System.out.println("Received store ack " + filename + " ");
                    
                    Integer remaining = fileToRemainingStoreACKs.get(filename);
                    fileToRemainingStoreACKs.put(filename, remaining - 1);
                    System.out.println("Store completed for port");
                }


                // ----------------------------LOAD--------------------------------------------

                // split into reload to reset the index

                    
                if (command.equals(Protocol.LOAD_TOKEN)) {

                    //  checks if there are enough dstores
                    if(countDstores < R) {

                        outputToClient.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                        ControllerLogger.getInstance().messageSent(client, Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);

                        continue;
                    }



                    System.out.println("Entering Load");

                    if(lineArgs.length != 2) {

                        System.err.println("Error, malformed message detected!");;
                        continue;
                    }

                    String filename = lineArgs[1];

                    portIndex = 0;

                    synchronized(this) {
                        if(removeQueue.contains(filename) || storeQueue.contains(filename))  {

                            
                            outputToClient.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                            ControllerLogger.getInstance().messageSent(client, Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);

                            continue;
                        }
                    }

                    if(fileToSize.get(filename) == null)  {
                        System.out.println("Load: file does not exist");

                        
                        outputToClient.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN); 
                        ControllerLogger.getInstance().messageSent(client, Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);

                    
                        continue;
                    }

                    ArrayList<Integer> portList = fileToPorts.get(filename);
                    if(portIndex <= portList.size() - 1 && portList != null) {
                    System.out.println("Selected port: " + portList.get(portIndex) + " index = " + portIndex);

                    outputToClient.println(Protocol.LOAD_FROM_TOKEN + " " + portList.get(portIndex) + " " + fileToSize.get(filename));
                    ControllerLogger.getInstance().messageSent(client, new String (Protocol.LOAD_FROM_TOKEN + " " + portList.get(portIndex) + " " + fileToSize.get(filename)));


                    } else if (portIndex >= portList.size()){
                        System.out.print("Error loading, index: " + portIndex);

                        outputToClient.println(Protocol.ERROR_LOAD_TOKEN);
                        ControllerLogger.getInstance().messageSent(client, Protocol.ERROR_LOAD_TOKEN);

                        continue;
                        }
                }




                // ----------------------------RELOAD--------------------------------------------

                // split into reload to reset the index
                
                if (command.equals(Protocol.RELOAD_TOKEN)) {

                    //  checks if there are enough dstores
                    if(countDstores < R) {

                        outputToClient.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                        ControllerLogger.getInstance().messageSent(client, Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);

                        continue;
                    }



                    System.out.println("Entering Load, portIndex: " + portIndex);
                    if(lineArgs.length != 2) {

                        System.err.println("Error, malformed message detected!");;
                        continue;
                    }

                    String filename = lineArgs[1];

                    portIndex++;

                    synchronized(this) {
                        if(removeQueue.contains(filename) || storeQueue.contains(filename) || fileToSize.get(filename) == null)  {


                                outputToClient.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);

                                //log reload
                                ControllerLogger.getInstance().messageSent(client, Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);

                                continue;
                        }
                    }

                    ArrayList<Integer> portList = fileToPorts.get(filename);
                    if(portIndex <= portList.size() - 1 && portList != null) {
                    System.out.println("Selected port: " + portList.get(portIndex) + " index = " + portIndex);

                    outputToClient.println(Protocol.LOAD_FROM_TOKEN + " " + portList.get(portIndex) + " " + fileToSize.get(filename));
                    ControllerLogger.getInstance().messageSent(client, new String(Protocol.LOAD_FROM_TOKEN + " " + portList.get(portIndex) + " " + fileToSize.get(filename)));

                    portIndex++;
                    
                    } else if (portIndex >= portList.size()){
                        System.out.print("Error loading, index: " + portIndex);
                        outputToClient.println(Protocol.ERROR_LOAD_TOKEN);
                        ControllerLogger.getInstance().messageSent(client, Protocol.ERROR_LOAD_TOKEN);

                        portIndex = 0;
                        continue;
                        }
                }



                // ----------------------------LIST--------------------------------------------
                if(command.equals("LIST")) {


                    //  checks if there are enough dstores
                    if(countDstores < R) {

                        outputToClient.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                        ControllerLogger.getInstance().messageSent(client, Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);

                        continue;
                    }


                    if(lineArgs.length != 1) {

                        System.err.println("Error, malformed message detected!");;
                        continue;
                    }

                    System.out.println("Sending List");
                    for (String file : fileToSize.keySet()) {
                        System.out.println("File name: " + file);
                    }
                    if(fileToSize.keySet().size() != 0) {
                        outputToClient.println(Protocol.LIST_TOKEN + " " + String.join(" ", fileToSize.keySet()));
                        ControllerLogger.getInstance().messageSent(client, new String(Protocol.LIST_TOKEN + " " + String.join(" ", fileToSize.keySet())));
                    } else  {
                        outputToClient.println(Protocol.LIST_TOKEN);
                        ControllerLogger.getInstance().messageSent(client, Protocol.LIST_TOKEN);
                    }

                }

                // ----------------------------REMOVE--------------------------------------------
                if (command.equals(Protocol.REMOVE_TOKEN)) {


                    //  checks if there are enough dstores
                    if(countDstores < R) {

                        outputToClient.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                        ControllerLogger.getInstance().messageSent(client, Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);

                        continue;
                    }                    
                    
                    if(lineArgs.length != 2) {

                        System.err.println("Error, malformed message detected!");;
                        continue;
                    }

                    String filename = lineArgs[1];

                    ControllerLogger.getInstance().messageReceived(client, Protocol.REMOVE_TOKEN);

                    // ----------------------------- REMOVE INDEX UPDATE
                    synchronized(this) {
                        if(!removeQueue.contains(filename) && !storeQueue.contains(filename) && fileToSize.keySet().contains(filename))  {

                        System.out.println("Entering Remove");
                        removeQueue.add(filename);
                        fileToSize.remove(filename);
                        fileToIndex.remove(filename);

                        } else {
                        System.out.println("Error Removing");
                        outputToClient.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                        ControllerLogger.getInstance().messageSent(client, Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);

                        continue;
                        }
                    }

                    
                    

                    fileToRemainingRemoveACKs.put(filename, R);

                    // goes over All of the ports and sends a message to Remove
                    for (Integer port : fileToPorts.get(filename)) {
                        Socket dstore = portToSocket.get(port);
                        
                        PrintWriter outputToDstore = new PrintWriter(new OutputStreamWriter(dstore.getOutputStream()), true);

                        outputToDstore.println(Protocol.REMOVE_TOKEN + " " + filename);
                        ControllerLogger.getInstance().messageSent(dstore, new String(Protocol.REMOVE_TOKEN + " " + filename));
                    }



                    // Vector<String> for files which need to be deleted

                    long startTime = System.currentTimeMillis();
                    System.out.println("Remove awaiting acknowledgements");
                    while(false||(System.currentTimeMillis()-startTime) < timeout) {
                        if(fileToRemainingRemoveACKs.get(filename) <= 0) {


                            System.out.println("All remove acks received... Sending remove_complete");
                            outputToClient.println(Protocol.REMOVE_COMPLETE_TOKEN);
                            ControllerLogger.getInstance().messageSent(client, Protocol.REMOVE_COMPLETE_TOKEN);

                            //Remove the values in the hashmaps
                            for (Integer port : fileToPorts.get(filename)) {
                                ArrayList<String> fileList = portToFiles.get(port);
                                fileList.remove(filename);
                                portToFiles.put(port, fileList);
                            }

                            fileToPorts.remove(filename);
                            removeQueue.remove(filename);

                            break;
                        } 
                    } 
                    
                }

                // ------------------------------REMOVE_ ACK---------------------------
                if (command.equals(Protocol.REMOVE_ACK_TOKEN)) {

                    if(lineArgs.length != 2) {

                        System.err.println("Error, malformed message detected!");;
                        continue;
                    }

                    String filename = lineArgs[1];
                    System.out.println("Received remove ack " + filename + " ");

                    ControllerLogger.getInstance().messageReceived(client, Protocol.REMOVE_ACK_TOKEN);

                    Integer remaining = fileToRemainingRemoveACKs.get(filename);
                    if(remaining == null) {
                        outputToClient.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                        ControllerLogger.getInstance().messageSent(client, Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                        continue;
                    }
                    fileToRemainingRemoveACKs.put(filename, remaining - 1);

                    System.out.println("Remove completed for port");
                }




            }
            
            } catch(Exception e){
                System.out.println("error "+e);
                e.printStackTrace();}
            }).start();
            }
        }catch(Exception e){
            System.out.println("error "+e);
            e.printStackTrace();}
        System.out.println();
    }
    public static void main(String[] args) throws IOException {

        Integer cport = Integer.parseInt(args[0]);
        Integer R = Integer.parseInt(args[1]);
        Integer timeout = Integer.parseInt(args[2]);
        Integer rebalance_period = Integer.parseInt(args[3]);

        Controller controller = new Controller();
        controller.initializeController(cport, R, timeout, rebalance_period);
        

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
