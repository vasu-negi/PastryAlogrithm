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
    Key: int []   
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
    
    | Join of NodeExtraData*int
    | Route of NodeExtraData
    | UpdateState of NodeExtraData * State * int  
    | Received of int * NodeExtraData 
    | FinishedSending
    | TotalNodesForInitialization of int
    
let mutable nodes =
    int (string (fsi.CommandLineArgs.GetValue 1))

let numRequests = string (fsi.CommandLineArgs.GetValue 2)
let system = ActorSystem.Create("System")
let mutable actualNumOfNodes = nodes |> float


let mutable  nodeArray = [||]
let b = 2
let numberOfRows = 128 / b
let numberOfCols = pown 2 b
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
        hash.[x] <- int(bigintHash % pow2)
        bigintHash <- bigintHash / pow2

    let mutable lastchangedtimestamp = 0
    
    let mutable numberOfneighborsFinishedSendingData = 0
    let mutable numberofNeighboringActors= 0

    let nodeId:NodeData = {
        Value = bigint(hashbyte)
        Key = hash
    }
    let selfNodeData = {
        IPreference = mailbox.Self 
        NodeId = nodeId
        IsnotNull = true
    }
    // Find Prefix length

    let findPrefixLength(sharedHash:int []) = 
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

        for i = 0 to numberOfRows - 1  do
            for j = 0 to numberOfCols - 1 do 
                updateSmallLeafSet(sharedState.RoutingTable.[i,j]) |> ignore
                updateSmallLeafSet(sharedState.RoutingTable.[i,j]) |> ignore 
                
        for lowerleaf in sharedState.SmallLeafSet do
            updateSmallLeafSet(lowerleaf) |> ignore
            updateSmallLeafSet(lowerleaf) |> ignore
        for largerleaf in sharedState.LargeLeafSet do
            updatelargeLeafSet(largerleaf) |> ignore
            updatelargeLeafSet(largerleaf) |> ignore

    let getNextNode (sharedData: NodeExtraData) = 
        let mutable sharedValue = sharedData.NodeId.Value 
        let mutable nextNodeToSend:NodeExtraData = NodeExtraData.Default
        if ((sharedValue >= state.SmallLeafSet.Min.NodeId.Value) && (sharedValue <= state.LargeLeafSet.Max.NodeId.Value)) then
            let mutable smallestDifference:bigint =  bigint(pown 2 b)
            for leaves in state.SmallLeafSet do
                if abs(leaves.NodeId.Value - sharedValue ) < smallestDifference then    
                    nextNodeToSend <- leaves 
                    smallestDifference <- abs(leaves.NodeId.Value - sharedValue )
            for leaves in state.LargeLeafSet do
                if abs(leaves.NodeId.Value - sharedValue ) < smallestDifference then    
                    nextNodeToSend <- leaves
                    smallestDifference <- abs(leaves.NodeId.Value - sharedValue)
        else 
            let mutable l = findPrefixLength(sharedData.NodeId.Key)
            if state.RoutingTable.[l,sharedData.NodeId.Key.[l]].IsnotNull then
                nextNodeToSend<- state.RoutingTable.[l,sharedData.NodeId.Key.[l]] 
            else
                let mutable difference:bigint = bigint(pown 2 b)
                for i = 0 to numberOfRows - 1  do
                    for j = 0 to numberOfCols - 1 do 
                        if findPrefixLength(state.RoutingTable.[i,j].NodeId.Key) >= l then
                            if abs(state.RoutingTable.[i,j].NodeId.Value - sharedValue) < difference then
                                nextNodeToSend <- state.RoutingTable.[i,j]
                                difference <-abs(state.RoutingTable.[i,j].NodeId.Value - sharedValue)
                for lowerleaf in state.SmallLeafSet do
                    if findPrefixLength(lowerleaf.NodeId.Key) >= l then
                        if abs(lowerleaf.NodeId.Value - sharedValue) < difference then
                            nextNodeToSend <- lowerleaf
                            difference <- abs(lowerleaf.NodeId.Value - sharedValue)
                for largerleaf in state.LargeLeafSet do
                    if findPrefixLength(largerleaf.NodeId.Key) >=l then
                        if abs(largerleaf.NodeId.Value - sharedValue) < difference  then
                            nextNodeToSend <- largerleaf
                            difference <- abs(largerleaf.NodeId.Value - sharedValue)
        nextNodeToSend

                
                
    let shareStatewithAll = 
        for i = 0 to numberOfRows - 1 do
            for j = 0 to numberOfCols - 1 do 
                state.RoutingTable.[i,j].IPreference <! UpdateState (selfNodeData,state, lastchangedtimestamp)
                
        for lowerleaf in state.SmallLeafSet do
            lowerleaf.IPreference <! UpdateState (selfNodeData,state, lastchangedtimestamp)

        for largerleaf in state.LargeLeafSet do
            largerleaf.IPreference <! UpdateState (selfNodeData,state, lastchangedtimestamp)
    

    let rec loop()= actor {
        let! message = mailbox.Receive();
        match message with
        | TotalNodesForInitialization (counter:int) ->
            numberofNeighboringActors <- counter

        | Join (x:NodeExtraData,counter:int) ->  
            let mutable nodeToSend = getNextNode x 
            if nodeToSend.IsnotNull then
                nodeToSend.IPreference <! Join (x, (counter+1))
                x.IPreference <! UpdateState (selfNodeData,state,lastchangedtimestamp)
            else 
                x.IPreference <! TotalNodesForInitialization counter
            
        | Route (x:NodeExtraData) ->
            
            if x.NodeId.Value = selfNodeData.NodeId.Value then
                printfn "Message Reached"
            let mutable nodeToSend = getNextNode x
            nodeToSend.IPreference<! Route x
            

        | UpdateState (receivedData:NodeExtraData,receivedState:State, recievedtimestamp:int) ->
            lastchangedtimestamp  <- lastchangedtimestamp + 1
            let mutable prefixlength = findPrefixLength receivedData.NodeId.Key // send the hash value (nodeId)
            //Update the table
            for i = 0 to prefixlength do
                for j = 0 to b do   
                    if (i <> selfNodeData.NodeId.Key.[j] && (state.RoutingTable.[i,j].IsnotNull) &&  (not receivedState.RoutingTable.[i,j].IsnotNull))  then
                        state.RoutingTable.[i,j] <- receivedState.RoutingTable.[i,j]
            //Updated the leafset
            updateLeafSet(receivedState, receivedData) 
            // Send received message with timestamp to the sender 
            receivedData.IPreference <! Received (recievedtimestamp,selfNodeData)

        | Received (recieved_timestamp, sharedData) ->
            if recieved_timestamp = lastchangedtimestamp then
                sharedData.IPreference <! FinishedSending
            elif recieved_timestamp < lastchangedtimestamp then  
                sharedData.IPreference <! UpdateState (selfNodeData,state,lastchangedtimestamp)

        | FinishedSending -> 
            numberOfneighborsFinishedSendingData <- numberOfneighborsFinishedSendingData + 1
            if numberOfneighborsFinishedSendingData = numberofNeighboringActors then
                shareStatewithAll 

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
