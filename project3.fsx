#r "nuget: Akka.FSharp" 
#r "nuget: Akka.TestKit" 

open System
open Akka.Actor
open Akka.FSharp
open System.Security.Cryptography
open System.Text
open System.Collections.Generic
open System.Globalization

///////////////////////////Initialization////////////////////////////////////////

[<Struct>]
type NodeStructure ={
    Value: bigint
    Key: int []   
}
with
    static member Default = {Value = bigint(-1) ; Key = null }

let bigintpower = bigint.Pow(2I,128)

    
let mutable nodes = int (string (fsi.CommandLineArgs.GetValue 1))
let numRequests =  int(string (fsi.CommandLineArgs.GetValue 2))
let system = ActorSystem.Create("System")
let mutable numberOfHops:int = 0

let b = 4
let numberOfRows = 128 / b
let numberOfCols = pown 2 b
let sizeofhalfleafset = 8


[<Struct>]
type NodeExtraData= {
    NodeId: NodeStructure
    IPreference: IActorRef
    IsnotNull: bool

}
with
    static member Default = { NodeId = NodeStructure.Default; IPreference = null; IsnotNull = false  } 

type State = {
    SmallLeafSet: SortedSet<NodeExtraData>
    LargeLeafSet: SortedSet<NodeExtraData>
    RoutingTable: NodeExtraData [,]
}

type StatusMessage = 
    | Initialization1
    | Initialization2
    | InitializationComplete
    
type Message = 
    | Join of NodeExtraData*int
    | Route of NodeExtraData*int
    | UpdateState of NodeExtraData * State * DateTime  
    | Received of DateTime * NodeExtraData 
    | FinishedSending
    | TotalNodesForInitialization of int
    | Init of IActorRef
    | SendMsgs
    

let getNodeExtraData (actorName: IActorRef) : NodeExtraData =
    let ipAddressofCurrentNode  =  System.Text.Encoding.ASCII.GetBytes actorName.Path.Name
    let md5 (data : byte array) : string =
        use md5 = MD5.Create()
        (StringBuilder(), md5.ComputeHash(data))
        ||> Array.fold (fun sb b -> sb.Append(b.ToString("X2")))
        |> string
    
    let mutable hashbyte =  md5 ipAddressofCurrentNode
    let mutable hash : int [] = Array.zeroCreate numberOfRows
    hashbyte <- "0" + hashbyte
    let pow2 = bigint.Pow(2I, b)
    let bigintHash = (bigint.Parse(hashbyte, NumberStyles.AllowHexSpecifier))
    
    let mutable hashValue = bigintHash
    for x in numberOfRows - 1 .. -1 .. 0  do 
        hash.[x] <- int(hashValue % pow2)
        hashValue <- hashValue / pow2

    let nodeId:NodeStructure = {
        Value = bigintHash
        Key = hash
    }

    let selfNodeStructure = {
        IPreference = actorName 
        NodeId = nodeId
        IsnotNull = true
    }
    selfNodeStructure
    
let mutable numInitComplete: int = 0
let mutable totalRoutedMsgs: int = 0
let mutable totalRoutedMsgsCount: int = 0

type MainActorMessageType = 
    | NodeInitComplete
    | MsgRoutedCorrectly of int
    
let MainActor (mailbox: Actor<_>) =
    let rec loop() = actor {
        let! msg = mailbox.Receive()

        match msg with 

        | MsgRoutedCorrectly count ->
            totalRoutedMsgs <- totalRoutedMsgs + 1
            totalRoutedMsgsCount <- totalRoutedMsgsCount + count - 1
        | NodeInitComplete ->
            numInitComplete <- numInitComplete + 1
        return! loop()
    }
    loop()

let MainActorRef = spawn system "MainActor" MainActor
///////////////////////////Initialization////////////////////////////////////////


///////////////////////////Worker Actor/ ///////////////////////////////////////
let Worker(mailbox: Actor<_>) =

    let ipAddressofCurrentNode  =  System.Text.Encoding.ASCII.GetBytes mailbox.Self.Path.Name
    let md5 (data : byte array) : string =
        use md5 = MD5.Create()
        (StringBuilder(), md5.ComputeHash(data))
        ||> Array.fold (fun sb b -> sb.Append(b.ToString("X2")))
        |> string

    let mutable StatusMessage: StatusMessage = Initialization1

    let mutable hashbyte =  md5 ipAddressofCurrentNode
    hashbyte <- "0" + hashbyte
    let bigintHash = (bigint.Parse(hashbyte, NumberStyles.AllowHexSpecifier))
    let mutable hash : int [] = Array.zeroCreate numberOfRows
    let mutable hashValue = bigintHash

    let pow2 = bigint.Pow(2I, b)
    let mutable lastchangedtimestamp:DateTime = DateTime.Now
    
    let mutable numberOfneighborsFinishedSendingData = 0
    let mutable numberofNeighboringActors= -1
    let mutable totalSentIn2ndStep = 0
    let mutable totalRecievedIn2ndStep= 0
    
    for x in numberOfRows - 1 .. -1 .. 0  do 
        hash.[x] <- int(hashValue % pow2)
        hashValue <- hashValue / pow2

    let nodeId:NodeStructure = {
        Value = bigintHash
        Key = hash
    }

    let selfNodeStructure = {
        IPreference = mailbox.Self 
        NodeId = nodeId
        IsnotNull = true
    }
        
    let findPrefixLength(hash1:int [],hash2:int []): int =
        let mutable prefix: int = 0;
        let rec cal i = 
            if i < numberOfRows && hash1.[i] = hash2.[i] then
                prefix <- prefix + 1
                cal (i+1)

        cal 0
        prefix


    let state:State = {
        SmallLeafSet =  SortedSet<NodeExtraData>()
        LargeLeafSet =  SortedSet<NodeExtraData>()
        RoutingTable = Array2D.create numberOfRows numberOfCols NodeExtraData.Default
    }

    // first look up in the lowerleafset, to insert the new data
    let updateSmallLeafSet (sharedData:NodeExtraData) =
        if sharedData.NodeId.Value < selfNodeStructure.NodeId.Value then 
            if state.SmallLeafSet.Count < sizeofhalfleafset then
                state.SmallLeafSet.Add(sharedData) |> ignore
                lastchangedtimestamp <- DateTime.Now
            else 
                let minValueInLeafSet = state.SmallLeafSet.Min
                if not (state.SmallLeafSet.Contains(sharedData) ) && sharedData.NodeId.Value > minValueInLeafSet.NodeId.Value then
                    state.SmallLeafSet.Remove(minValueInLeafSet) |> ignore
                    state.SmallLeafSet.Add(sharedData) |> ignore
                    lastchangedtimestamp  <- DateTime.Now

    //  look up in the largerleafset, to insert the new data
    let updatelargeLeafSet (sharedData:NodeExtraData) =
        if sharedData.NodeId.Value > selfNodeStructure.NodeId.Value then 
            if state.LargeLeafSet.Count < sizeofhalfleafset then
                state.LargeLeafSet.Add(sharedData) |> ignore
                lastchangedtimestamp <- DateTime.Now
            else 
                let maxValueInLeafSet = state.LargeLeafSet.Max
                if not (state.LargeLeafSet.Contains(sharedData) ) && sharedData.NodeId.Value < maxValueInLeafSet.NodeId.Value then
                    state.LargeLeafSet.Remove(maxValueInLeafSet) |> ignore
                    state.LargeLeafSet.Add(sharedData) |> ignore
                    lastchangedtimestamp  <- DateTime.Now

    // check the routing table, and leafset of the sharedData to put values into new Actor (Visiting) leafset
    let updateLeafSet (sharedState:State,sharedData:NodeExtraData) = 

        for i = 0 to numberOfRows - 1  do
            for j = 0 to numberOfCols - 1 do 
                if sharedState.RoutingTable.[i,j].IsnotNull then
                    updateSmallLeafSet(sharedState.RoutingTable.[i,j])
                    updatelargeLeafSet(sharedState.RoutingTable.[i,j])
                    
        for lowerleaf in sharedState.SmallLeafSet do
            updateSmallLeafSet(lowerleaf)
            updatelargeLeafSet(lowerleaf)
        for largerleaf in sharedState.LargeLeafSet do
            updateSmallLeafSet(largerleaf)
            updatelargeLeafSet(largerleaf)

        updateSmallLeafSet sharedData
        updatelargeLeafSet sharedData

    // retreieves the random node based on the routing alogrithm 
    let getrandomNodeToRoute (sharedData: NodeExtraData) = 

        let mutable sharedValue = sharedData.NodeId.Value 
        let mutable randomNodeToSend:NodeExtraData = NodeExtraData.Default
        if selfNodeStructure.NodeId.Value = sharedData.NodeId.Value then
            randomNodeToSend <- NodeExtraData.Default
        elif state.SmallLeafSet.Count > 0 && state.LargeLeafSet.Count >0 && sharedValue >= state.SmallLeafSet.Min.NodeId.Value && sharedValue <= state.LargeLeafSet.Max.NodeId.Value then
            let mutable smallestDifference:bigint = bigint.Abs (selfNodeStructure.NodeId.Value - sharedValue)
            for leaves in state.SmallLeafSet do
                if bigint.Abs(leaves.NodeId.Value - sharedValue ) < smallestDifference then    
                    randomNodeToSend <- leaves 
                    smallestDifference <- bigint.Abs(leaves.NodeId.Value - sharedValue )

            for leaves in state.LargeLeafSet do
                if bigint.Abs(leaves.NodeId.Value - sharedValue ) < smallestDifference then    
                    randomNodeToSend <- leaves 
                    smallestDifference <- bigint.Abs(leaves.NodeId.Value - sharedValue )
        else 
            let mutable l = findPrefixLength(sharedData.NodeId.Key, selfNodeStructure.NodeId.Key) // D 
            if state.RoutingTable.[l, sharedData.NodeId.Key.[l]].IsnotNull then
                randomNodeToSend <- state.RoutingTable.[l,sharedData.NodeId.Key.[l]] 
            else
                let mutable difference:bigint = bigint.Abs(selfNodeStructure.NodeId.Value - sharedValue)

                for i = 0 to numberOfRows - 1  do
                    for j = 0 to numberOfCols - 1 do 
                        if state.RoutingTable.[i,j].IsnotNull && findPrefixLength(state.RoutingTable.[i,j].NodeId.Key, sharedData.NodeId.Key) >= l then
                            if bigint.Abs(state.RoutingTable.[i,j].NodeId.Value - sharedValue) < difference then
                                randomNodeToSend <- state.RoutingTable.[i,j]
                                difference <-bigint.Abs(state.RoutingTable.[i,j].NodeId.Value - sharedValue)


                for lowerleaf in state.SmallLeafSet do
                    if findPrefixLength(lowerleaf.NodeId.Key,sharedData.NodeId.Key) >= l then
                        if bigint.Abs(lowerleaf.NodeId.Value - sharedValue) < difference then
                            randomNodeToSend <- lowerleaf
                            difference <- bigint.Abs(lowerleaf.NodeId.Value - sharedValue)


                for largerleaf in state.LargeLeafSet do
                    if findPrefixLength(largerleaf.NodeId.Key, sharedData.NodeId.Key) >=l then
                        if bigint.Abs(largerleaf.NodeId.Value - sharedValue) < difference  then
                            randomNodeToSend <- largerleaf
                            difference <- bigint.Abs(largerleaf.NodeId.Value - sharedValue)

        randomNodeToSend

                
    // Share state with all the neighboring 
    let shareStatewithAll () = 
        
        for i = 0 to numberOfRows - 1 do
            for j = 0 to numberOfCols - 1 do 
                if state.RoutingTable.[i,j].IsnotNull then
                    state.RoutingTable.[i,j].IPreference <! UpdateState (selfNodeStructure, state, lastchangedtimestamp)
                    totalSentIn2ndStep <- totalSentIn2ndStep + 1
                
        for lowerleaf in state.SmallLeafSet do
            lowerleaf.IPreference <! UpdateState (selfNodeStructure, state, lastchangedtimestamp)
            totalSentIn2ndStep <- totalSentIn2ndStep + 1

        for largerleaf in state.LargeLeafSet do
            largerleaf.IPreference <! UpdateState (selfNodeStructure, state, lastchangedtimestamp)
            totalSentIn2ndStep <- totalSentIn2ndStep + 1
    

    let rec loop()= actor {
        let! message = mailbox.Receive();
        match message with
        | Init actorref ->
            if isNull actorref then
                MainActorRef <! NodeInitComplete
                StatusMessage <- InitializationComplete
            else 
                actorref <! Join (selfNodeStructure,0)

        | TotalNodesForInitialization (counter:int) ->
            numberofNeighboringActors <- counter

        | Join (x:NodeExtraData,counter:int) ->  
            let mutable nodeToSend = getrandomNodeToRoute x 
            if nodeToSend.IsnotNull then
                nodeToSend.IPreference <! Join (x, (counter+1))
            else 
                // Reached Z, send number of total neighboractors to new actor X 
                x.IPreference <! TotalNodesForInitialization (counter+1)
            x.IPreference <! UpdateState (selfNodeStructure,state,lastchangedtimestamp)
            
        | Route (x:NodeExtraData, counter: int) ->
            
            if x.NodeId.Value = selfNodeStructure.NodeId.Value then
                MainActorRef <! MsgRoutedCorrectly counter
            else
                let mutable nodeToSend = getrandomNodeToRoute x 
                nodeToSend.IPreference<! Route (x, counter + 1)
        
                
                
            
        // Update own state based on the state received
        | UpdateState (receivedData:NodeExtraData,receivedState:State, recievedtimestamp:DateTime) ->
            
            let mutable prefixlength = findPrefixLength (receivedData.NodeId.Key, selfNodeStructure.NodeId.Key )// send the hash value (nodeId)
            //Update the table
            for i = 0 to prefixlength do
                for j = 0 to b - 1 do   
                    if (j <> receivedData.NodeId.Key.[i] && (not state.RoutingTable.[i,j].IsnotNull) &&  (receivedState.RoutingTable.[i,j].IsnotNull))  then
                        state.RoutingTable.[i,j] <- receivedState.RoutingTable.[i,j]
                        lastchangedtimestamp  <- DateTime.Now

            if not state.RoutingTable.[0, receivedData.NodeId.Key.[0]].IsnotNull then
                state.RoutingTable.[0, receivedData.NodeId.Key.[0]] <- receivedData
            
            // state.RoutingTable.[prefixlength, receivedData.NodeId.Key.[prefixlength]] <- receivedData
            
            // Updating the table from the shared smaller leaf set.
            for leaf in receivedState.SmallLeafSet do
                let l = findPrefixLength(hash, leaf.NodeId.Key)
                if leaf <> selfNodeStructure && not state.RoutingTable.[l, leaf.NodeId.Key.[l]].IsnotNull then
                    state.RoutingTable.[l, leaf.NodeId.Key.[l]] <- leaf

            // Updating the table from the shared larger leaf set.
            for leaf in receivedState.LargeLeafSet do
                let l = findPrefixLength(hash, leaf.NodeId.Key)
                if leaf <> selfNodeStructure && not state.RoutingTable.[l, leaf.NodeId.Key.[l]].IsnotNull then
                    state.RoutingTable.[l, leaf.NodeId.Key.[l]] <- leaf

            //Updated the leafset
            updateLeafSet(receivedState, receivedData) 

            // Send received message with timestamp to the sender 
            receivedData.IPreference <! Received (recievedtimestamp, selfNodeStructure)
        // Send a message to the neighbor A...Z that the messgage has been received
        | Received (recieved_timestamp:DateTime, sharedData) ->
            
            if recieved_timestamp.Equals lastchangedtimestamp then
                if StatusMessage = Initialization2 then
                    totalRecievedIn2ndStep <- totalRecievedIn2ndStep + 1
                    if totalSentIn2ndStep = totalRecievedIn2ndStep then
                        StatusMessage <- InitializationComplete
                        // Notify the supervisor in this case
                        MainActorRef <! NodeInitComplete
                sharedData.IPreference <! FinishedSending
            elif recieved_timestamp < lastchangedtimestamp then  
                sharedData.IPreference <! UpdateState (selfNodeStructure,state,lastchangedtimestamp)
        // If all the neighbors have shared the state with the new neighbor X, then send new neighbor's state to all the neighbors
        | FinishedSending -> 
            
            if StatusMessage = Initialization1 then
                numberOfneighborsFinishedSendingData <- numberOfneighborsFinishedSendingData + 1
                if numberOfneighborsFinishedSendingData = numberofNeighboringActors then
                    StatusMessage <- Initialization2
                    shareStatewithAll()

        return! loop()
        
    }            
    loop()
  

///////////////////////////Worker Actor////////////////////////////////////////

///////////////////////////Program////////////////////////////////////////
let nodeArray = Array.zeroCreate (nodes )
let mutable numberofRequestsDone = 0
for x in 0 .. nodes - 1 do
    nodeArray.[x] <- spawn system ((string) x) Worker


let ActorForSender (mailbox: Actor<_>) =
    let mutable sourceForParty: IActorRef = null
    
    let mutable completedRequests = 0
    let random = Random()
    let rec loop() = actor {
        let! msg = mailbox.Receive()

        match msg with 
            | SendMsgs ->
                if completedRequests < numRequests then
                    let mutable randomV = random.Next(0, nodes)
                    completedRequests <- completedRequests + 1
                    while(nodeArray.[randomV] = sourceForParty ) do
                        randomV <- random.Next(0, nodes)
                    sourceForParty <!  Route (getNodeExtraData(nodeArray.[randomV]), 0)
                    let t = new Timers.Timer(1000.0)
                    t.Elapsed.Add (fun _ -> mailbox.Self <! SendMsgs )
                    t.Start()

            | Init node ->
               sourceForParty <- node
            | _ -> 
                ()
                
            
        return! loop()
    }
    loop()


let listOfActorForSender = Array.zeroCreate (nodes )
for x in 0 .. nodes - 1 do
    listOfActorForSender.[x] <- spawn system ("ActorForSender" + (string) x) ActorForSender

let random = Random()

for x in [0 .. nodes - 1] do
    if x = 0 then
        nodeArray.[x] <! Init null
    else 
        let randomNodeVal = random.Next(0, x)
        nodeArray.[x] <! Init nodeArray.[randomNodeVal]
    while (numInitComplete <> x + 1) do
        ()  
    listOfActorForSender.[x] <! Init nodeArray.[x]


let mutable randomV = int 0
for x in [0 .. nodes - 1] do
    for _ = 1 to numRequests do
        randomV <- random.Next(0, nodes)
        while(randomV = x) do
            randomV <- random.Next(0, nodes)
        nodeArray.[x] <! Route (getNodeExtraData(nodeArray.[randomV]), 0)


while (totalRoutedMsgs <> nodes * numRequests) do
    ()

printfn "Pastry Algorithm"

printfn "Avg number of hops %A" (((double) totalRoutedMsgsCount)/((double)totalRoutedMsgs))



///////////////////////////Program////////////////////////////////////////
