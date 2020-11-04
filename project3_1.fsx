#r "nuget: Akka.FSharp" 
#r "nuget: Akka.TestKit" 

open System
open Akka.Actor
open Akka.FSharp
open System.Security.Cryptography
open System.Text
open System.Collections.Generic
///////////////////////////Initialization////////////////////////////////////////
type NodeData ={
    Value: bigint
    Key: byte []
    
}
with
    static member Default = {Key = null ; Value = bigint(null)}

type NodeExtraData= {
    NodeId: NodeData
    IPreference: IActorRef
    IsnotNull: bool

}
with
    static member Default = { NodeId = NodeData.Default; IPreference = null;IsnotNull = false  } 


type State = {
    SmallLeafSet: SortedSet<NodeExtraData>
    LargeLeafSet: SortedSet<NodeExtraData>
    RoutingTable: NodeExtraData [,]
}
with static member Default = { SmallLeafSet = null; LargeLeafSet = null; RoutingTable= null } 

type Message = 
    

    | Join of IActorRef
    | Route of IActorRef
    | UpdateState of IActorRef*NodeExtraData * State * int  
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
    let mutable hashbyte = MD5.Create().ComputeHash(ipAddressofCurrentNode)
    let mutable bigintHash = bigint(hashbyte)
    let mutable hash : int [] = Array.zeroCreate b
    let pow2 = bigint(pown 2 b)
    for x = b-1 to 0 do 
        hash.[x] <- bigintHash % pow2   
        
 



    let mutable lastchangedtimestamp = 0

    let selfNodeData = {
        IPreference = mailbox.Self 
        NodeId = {
            Value = bigint(hash)
            Key = MD5.Create().ComputeHash(ipAddressofCurrentNode)
        }
        IsnotNull = true
    }
    // Find Prefix length
    let findPrefixLength(sharedHash:byte []) = 
        let mutable prefix = 0
        for i in 0..hash.Length-1 do
            if hash.[i] = sharedHash.[i] then
                if prefix = i then
                    prefix <- prefix + 1
        prefix 
    
    let state:State = {
        SmallLeafSet =  SortedSet<NodeExtraData>()
        LargeLeafSet =  SortedSet<NodeExtraData>()
        RoutingTable = Array2D.create numberOfRows numberOfCols NodeExtraData.Default
    }
    let updateSmallLeafSet (sharedData:NodeExtraData) =
        if sharedData.NodeId.Value < selfNodeData.NodeId.Value then 
            if state.SmallLeafSet.Count < sizeofhalfleafset then
                state.SmallLeafSet.Add(sharedData) |> ignore
            else 
                let minValueInLeafSet = state.SmallLeafSet.Min
                state.SmallLeafSet.Remove(minValueInLeafSet) |> ignore
                state.SmallLeafSet.Add(sharedData) |> ignore

    let updatelargeLeafSet (sharedData:NodeExtraData) =
        if sharedData.NodeId.Value > selfNodeData.NodeId.Value then 
            if state.LargeLeafSet.Count < sizeofhalfleafset then
                state.LargeLeafSet.Add(sharedData) |> ignore
            else 
                let maxValueInLeafSet = state.LargeLeafSet.Max
                state.LargeLeafSet.Remove(maxValueInLeafSet) |> ignore
                state.LargeLeafSet.Add(sharedData) |> ignore
                    
    let updateLeafSet (sharedState:State,sharedData:NodeExtraData) = 

        for i = 0 to numberOfRows do
            for j = 0 to numberOfCols do 
                updateSmallLeafSet(sharedState.RoutingTable.[i,j]) |> ignore
                updateSmallLeafSet(sharedState.RoutingTable.[i,j]) |> ignore 
                
        for lowerleaf in sharedState.SmallLeafSet do
            updateSmallLeafSet(lowerleaf) |> ignore
            updateSmallLeafSet(lowerleaf) |> ignore
        for largerleaf in sharedState.LargeLeafSet do
            updatelargeLeafSet(largerleaf) |> ignore
            updatelargeLeafSet(largerleaf) |> ignore

    let rec loop()= actor {
        let! message = mailbox.Receive();
        
        
        match message with

        | Join x -> 
            //nodeToSend = computeRouting x 
            nodeToSend <! Join x
            x <! UpdateState (mailbox.Self,selfNodeData,state,lastchangedtimestamp)
        
        | Route x ->
            //nodeToSend = computeRouting x
            nodeToSend<! Route x

        | UpdateState (referenceofSender:IActorRef,receivedData:NodeExtraData,receivedState:State, recievedtimestamp:int) ->
            lastchangedtimestamp  <- lastchangedtimestamp + 1
            let mutable prefixlength = findPrefixLength receivedData.NodeId.Key // send the hash value (nodeId)
            
            //Update the table
            for i = 0 to prefixlength do
                for j = 0 to b do   
                    if (i <> selfNodeData.NodeId.Key.[i,j] && (state.RoutingTable.[i,j].IsnotNull) &&  (not receivedState.RoutingTable.[i,j].IsnotNull))  then
                        state.RoutingTable.[i,j] <- receivedState.RoutingTable.[i,j]
            //Updated the leafset
            updateLeafSet(receivedState, receivedData) 
            // send received message with timestamp to the sender
            referenceofSender <! Received (recievedtimestamp,mailbox.Self)
            
        | Received (recieved_timestamp, referenceofSender) ->
            if recieved_timestamp < lastchangedtimestamp then  
                referenceofSender <! UpdateState (mailbox.Self,selfNodeData,state,lastchangedtimestamp)
        
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
