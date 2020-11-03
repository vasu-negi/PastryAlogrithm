#r "nuget: Akka.FSharp" 
#r "nuget: Akka.TestKit" 

open System
open Akka.Actor

open Akka.FSharp
open System.Security.Cryptography
open System.Text
///////////////////////////Initialization////////////////////////////////////////


type NodeExtraData= {
    NodeId: NodeData
    IPreference: IActorRef

}
and NodeData ={
    Key: byte []
    Value: bigint 
    
}
type State = {
    SmallLeafSet:NodeExtraData [] 
    LargeLeafSet:NodeExtraData []
    RoutingTable: NodeExtraData [,]
}

type Message = 
    
    | Join of IActorRef
    | Route of IActorRef
    | UpdateState of IActorRef*State * int
    | Received of int * IActorRef 

let mutable nodes =
    int (string (fsi.CommandLineArgs.GetValue 1))

let numRequests = string (fsi.CommandLineArgs.GetValue 2)
let system = ActorSystem.Create("System")
let mutable actualNumOfNodes = nodes |> float


let mutable  nodeArray = [||]
let b = 2
let numberOfRows = 128 / b
let numberOfCols = b
let sizeofhalfleafset = 8

///////////////////////////Initialization////////////////////////////////////////



///////////////////////////Worker Actor/ ///////////////////////////////////////
let Worker(mailbox: Actor<_>) =
    let ipAddressofCurrentNode  =  System.Text.Encoding.ASCII.GetBytes mailbox.Self.Path.Name
    let hash = MD5.Create().ComputeHash(ipAddressofCurrentNode)
    let mutable lastchangedtimestamp = 0
    let selfNodeData = {
        IPreference = mailbox.Self 
        NodeId = {
            Key = MD5.Create().ComputeHash(ipAddressofCurrentNode)
            Value = bigint(hash)
        }
    }

    let state:State ={
        SmallLeafSet = Array.zeroCreate sizeofhalfleafset
        LargeLeafSet = Array.zeroCreate sizeofhalfleafset
        RoutingTable = Array2D.zeroCreate numberOfRows numberOfCols
      }
    
    let rec loop()= actor{
        let! message = mailbox.Receive();
            
        match message with
        | Join x -> 
            nodeToSend = computeRouting x 
            nodeToSend <! Join x
            x <! UpdateState (mailbox.Self,state,lastchangedtimestamp)
        
        | Route x ->
            nodeToSend = computeRouting x
            nodeToSend<! Route x

        | UpdateState (referenceofSender,recievedState,recievedtimestamp)->
            lastchangedtimestamp  <- lastchangedtimestamp + 1
            // update the routing table
            referenceofSender <! Received (recievedtimestamp,mailbox.Self)
            
        | Received (recieved_timestamp, referenceofSender) ->
            if recieved_timestamp < lastchangedtimestamp then  
                referenceofSender <! UpdateState (mailbox.Self,state,lastchangedtimestamp)
        
        return! loop()
    }            
    loop()


///////////////////////////Worker Actor////////////////////////////////////////

///////////////////////////Program////////////////////////////////////////

nodeArray <- Array.zeroCreate (nodes + 1)


for x in [0..nodes] do
    let actorkey : string = "actor" + string(x) 
    let actorRef = spawn system (actorkey) Worker
    let value: string = "Actor invoked " + string(x)
    nodeArray.[x] <- actorRef 
    

Console.ReadLine() |> ignore
///////////////////////////Program////////////////////////////////////////
