


/*
TODO:


-----------------------------STORE---------------------------

Client -> Controller "Store filename filesize"
Controller -> Client "Store_to port1 port2 ... portR"

CLIENT -> Dstore "Store filename filesize"
Dstore -> Client ACK
Client -> Dstore file_content
Dstore -> Controller "Store_ACK filename"
 
Controller -> Client "STORE_Complete" (when received all acks)

---------------------------------------------------------------


-----------------------------LOAD---------------------------

Client -> Controller "LOAD filename" 

(Controller selects one of the dstores with the file)

Controller->Client: "LOAD_FROM port filesize"
Client -> Dstore: "LOAD_DATA filename"

Dstore -> Client: file_content

--------------------------------------------------------------

-----------------------------REMOVE---------------------------

Client -> Controller "REMOVE filename"

For each Dstore i storing filename 
Controller->Dstore i "REMOVE filename"

Once Dstore i finishes removing the file,
Dstore i -> Controller "REMOVE_ACK filename"

Controller -> Client:REMOVE_COMPLETE

--------------------------------------------------------------

Client->Controller: LIST
Controller->Client:LIST file_list
file_list is a space-separated list of filenames

*/












public class App {
    public static void main(String[] args) throws Exception {
        System.out.println("Hello, World!");
    }
}
