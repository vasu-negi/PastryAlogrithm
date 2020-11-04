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
type NodeData ={
    Value: bigint
    Key: int []   
}
with
    static member Default = {Value = bigint(-1) ; Key = null }
    
[<Struct>]
type NodeExtraData= {
    NodeId: NodeData
    IPreference: IActorRef
    IsnotNull: bool

}
with
    static member Default = { NodeId = NodeData.Default; IPreference = null; IsnotNull = false  } 

let bigintpower = bigint.Pow(2I,128)

type State = {
    SmallLeafSet: SortedSet<NodeExtraData>
    LargeLeafSet: SortedSet<NodeExtraData>
    RoutingTable: NodeExtraData [,]
}

type Status = 
    | InitializationFirstStep
    | InitializationSecondStep
    | InitializationDone

type Message = 
    | Join of NodeExtraData*int
    | Route of NodeExtraData*int
    | UpdateState of NodeExtraData * State * DateTime  
    | Received of DateTime * NodeExtraData 
    | FinishedSending
    | TotalNodesForInitialization of int
    | Init of IActorRef
    
type DriverActorMessage =
    | FinishedSRouting
    
    
let mutable nodes = int (string (fsi.CommandLineArgs.GetValue 1))
let numRequests =  int(string (fsi.CommandLineArgs.GetValue 2))
let system = ActorSystem.Create("System")
let mutable numberOfHops:int = 0

let b = 4
let numberOfRows = 128 / b
let numberOfCols = pown 2 b
let sizeofhalfleafset = 8

let getNodeId (actorName: IActorRef) : NodeExtraData =
    let ipAddressofCurrentNode  =  System.Text.Encoding.ASCII.GetBytes actorName.Path.Name
    let md5 (data : byte array) : string =
        use md5 = MD5.Create()
        (StringBuilder(), md5.ComputeHash(data))
        ||> Array.fold (fun sb b -> sb.Append(b.ToString("X2")))
        |> string
    
    
    let mutable hashbyte =  md5 ipAddressofCurrentNode
    hashbyte <- "0" + hashbyte
    let bigintHash = (bigint.Parse(hashbyte, NumberStyles.AllowHexSpecifier))
    
    let mutable temp = bigintHash
    let mutable hash : int [] = Array.zeroCreate numberOfRows
    let pow2 = bigint(pown 2 b)

    for x in numberOfRows - 1 .. -1 .. 0  do 
        hash.[x] <- int(temp % pow2)
        temp <- temp / pow2
    let nodeId:NodeData = {
        Value = bigintHash
        Key = hash
    }

    let selfNodeData = {
        IPreference = actorName 
        NodeId = nodeId
        IsnotNull = true
    }
    selfNodeData
let mutable numInitializationDone: int = 0
let mutable totalRoutedMessages: int = 0
let mutable totalRoutedMessagesCount: int = 0


type DriverActorMessageType = 
    | NodeInitializationDone
    | MessageRoutedSuccessfully of int
    | DriverPrintState of State * NodeExtraData

// Driver Actor
let DriverActor (mailbox: Actor<_>) =
    let rec loop() = actor {
        let! msg = mailbox.Receive()

        match msg with 
        | NodeInitializationDone ->
            numInitializationDone <- numInitializationDone + 1

        | MessageRoutedSuccessfully count ->
            totalRoutedMessages <- totalRoutedMessages + 1
            totalRoutedMessagesCount <- totalRoutedMessagesCount + count

        | DriverPrintState (state, metadata) -> 
            printfn ""
            printfn "*************Printing %A *************" metadata.NodeId.Value
            printfn "Smaller LeafSet:"
            for leaf in state.SmallLeafSet do
                printf "%A " leaf.NodeId.Value

            printfn ""
            printfn "Larger LeafSet"
            for leaf in state.LargeLeafSet do
                printf "%A " leaf.NodeId.Value

            printfn ""
            printfn "Routirng Table"
            for i in 0 .. numberOfRows - 1 do
                for j in 0 .. numberOfCols - 1 do
                    printf "%A " state.RoutingTable.[i,j].NodeId.Value
                printfn "" 

        return! loop()
    }
    loop()

let driverActorRef = spawn system "DriverActor" DriverActor
///////////////////////////Initialization////////////////////////////////////////


///////////////////////////Worker Actor/ ///////////////////////////////////////
let Worker(mailbox: Actor<_>) =

    let ipAddressofCurrentNode  =  System.Text.Encoding.ASCII.GetBytes mailbox.Self.Path.Name
    let md5 (data : byte array) : string =
        use md5 = MD5.Create()
        (StringBuilder(), md5.ComputeHash(data))
        ||> Array.fold (fun sb b -> sb.Append(b.ToString("X2")))
        |> string

    let mutable status: Status = InitializationFirstStep

    
    
    let mutable hashbyte =  md5 ipAddressofCurrentNode
    hashbyte <- "0" + hashbyte
    let bigintHash = (bigint.Parse(hashbyte, NumberStyles.AllowHexSpecifier))
    
    let mutable temp = bigintHash
    let mutable hash : int [] = Array.zeroCreate numberOfRows
    let pow2 = bigint(pown 2 b)
    for x in numberOfRows - 1 .. -1 .. 0  do 
        hash.[x] <- int(temp % pow2)
        temp <- temp / pow2

    let mutable lastchangedtimestamp:DateTime = DateTime.Now
    let mutable numberOfneighborsFinishedSendingData = 0
    let mutable numberofNeighboringActors= -1
    
    let mutable totalSentIn2ndStep = 0
    let mutable totalRecievedIn2ndStep= 0 //total neighbors sending data back to the new Node
    
    let nodeId:NodeData = {
        Value = bigintHash
        Key = hash
    }

    let selfNodeData = {
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
        if sharedData.NodeId.Value < selfNodeData.NodeId.Value then 
            if state.SmallLeafSet.Count < sizeofhalfleafset then
                state.SmallLeafSet.Add(sharedData) |> ignore
                lastchangedtimestamp  <- DateTime.Now
            else 
                let minValueInLeafSet = state.SmallLeafSet.Min
                if not (state.SmallLeafSet.Contains(sharedData) ) && sharedData.NodeId.Value > minValueInLeafSet.NodeId.Value then
                    state.SmallLeafSet.Remove(minValueInLeafSet) |> ignore
                    state.SmallLeafSet.Add(sharedData) |> ignore
                    lastchangedtimestamp  <- DateTime.Now

    //  look up in the largerleafset, to insert the new data
    let updatelargeLeafSet (sharedData:NodeExtraData) =
        if sharedData.NodeId.Value > selfNodeData.NodeId.Value then 
            if state.LargeLeafSet.Count < sizeofhalfleafset then
                state.LargeLeafSet.Add(sharedData) |> ignore
                lastchangedtimestamp  <- DateTime.Now
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
                    updateSmallLeafSet(sharedState.RoutingTable.[i,j]) |> ignore
                    updateSmallLeafSet(sharedState.RoutingTable.[i,j]) |> ignore 
                    
        for lowerleaf in sharedState.SmallLeafSet do
            updateSmallLeafSet(lowerleaf) |> ignore
            updatelargeLeafSet(lowerleaf) |> ignore
        for largerleaf in sharedState.LargeLeafSet do
            updateSmallLeafSet(largerleaf) |> ignore
            updatelargeLeafSet(largerleaf) |> ignore

        updateSmallLeafSet sharedData
        updatelargeLeafSet sharedData

    // retreieves the next node based on the routing alogrithm 
    let getNextNode (sharedData: NodeExtraData) = 
        let mutable sharedValue = sharedData.NodeId.Value 
        let mutable nextNodeToSend:NodeExtraData = NodeExtraData.Default

        if (sharedValue <> selfNodeData.NodeId.Value) then

            if ((sharedValue >= state.SmallLeafSet.Min.NodeId.Value) && (sharedValue <= state.LargeLeafSet.Max.NodeId.Value)) then
                let mutable smallestDifference:bigint = bigint.Abs (selfNodeData.NodeId.Value - sharedValue)
                for leaves in state.SmallLeafSet do
                    if bigint.Abs(leaves.NodeId.Value - sharedValue ) < smallestDifference then    
                        nextNodeToSend <- leaves 
                        smallestDifference <- bigint.Abs(leaves.NodeId.Value - sharedValue )

                for leaves in state.LargeLeafSet do
                    if bigint.Abs(leaves.NodeId.Value - sharedValue ) < smallestDifference then    
                        nextNodeToSend <- leaves 
                        smallestDifference <- bigint.Abs(leaves.NodeId.Value - sharedValue )
            else 
                let mutable l = findPrefixLength(sharedData.NodeId.Key, selfNodeData.NodeId.Key) // D 
                if state.RoutingTable.[l,sharedData.NodeId.Key.[l]].IsnotNull then
                    nextNodeToSend<- state.RoutingTable.[l,sharedData.NodeId.Key.[l]] 
                else
                    let mutable difference:bigint = bigint.Abs(selfNodeData.NodeId.Value - sharedValue)

                    for i = 0 to numberOfRows - 1  do
                        for j = 0 to numberOfCols - 1 do 
                            if state.RoutingTable.[i,j].IsnotNull && findPrefixLength(state.RoutingTable.[i,j].NodeId.Key, sharedData.NodeId.Key) >= l then
                                if bigint.Abs(state.RoutingTable.[i,j].NodeId.Value - sharedValue) < difference then
                                    nextNodeToSend <- state.RoutingTable.[i,j]
                                    difference <-bigint.Abs(state.RoutingTable.[i,j].NodeId.Value - sharedValue)


                    for lowerleaf in state.SmallLeafSet do
                        if findPrefixLength(lowerleaf.NodeId.Key,sharedData.NodeId.Key) >= l then
                            if bigint.Abs(lowerleaf.NodeId.Value - sharedValue) < difference then
                                nextNodeToSend <- lowerleaf
                                difference <- bigint.Abs(lowerleaf.NodeId.Value - sharedValue)


                    for largerleaf in state.LargeLeafSet do
                        if findPrefixLength(largerleaf.NodeId.Key, sharedData.NodeId.Key) >=l then
                            if bigint.Abs(largerleaf.NodeId.Value - sharedValue) < difference  then
                                nextNodeToSend <- largerleaf
                                difference <- bigint.Abs(largerleaf.NodeId.Value - sharedValue)
        nextNodeToSend

                
    // Share state with all the neighboring  
    let shareStatewithAll () = 
        
        for i = 0 to numberOfRows - 1 do
            for j = 0 to numberOfCols - 1 do 
                if state.RoutingTable.[i,j].IsnotNull then
                    state.RoutingTable.[i,j].IPreference <! UpdateState (selfNodeData,state, lastchangedtimestamp)
                    totalSentIn2ndStep <- totalSentIn2ndStep + 1
                
        for lowerleaf in state.SmallLeafSet do
            lowerleaf.IPreference <! UpdateState (selfNodeData,state, lastchangedtimestamp)
            totalSentIn2ndStep <- totalSentIn2ndStep + 1

        for largerleaf in state.LargeLeafSet do
            largerleaf.IPreference <! UpdateState (selfNodeData,state, lastchangedtimestamp)
            totalSentIn2ndStep <- totalSentIn2ndStep + 1
    

    let rec loop()= actor {
        let! message = mailbox.Receive();
        match message with
        | Init actorref ->
            if isNull actorref then
                driverActorRef <! NodeInitializationDone
                status <- InitializationDone
            else 
                actorref <! Join (selfNodeData,0)

        | TotalNodesForInitialization (counter:int) ->
            numberofNeighboringActors <- counter

        | Join (x:NodeExtraData,counter:int) ->  
            let mutable nodeToSend = getNextNode x 
            if nodeToSend.IsnotNull then
                nodeToSend.IPreference <! Join (x, (counter+1))
            else 
                // Reached Z, send number of total neighboractors to new actor X 
                x.IPreference <! TotalNodesForInitialization (counter+1)
            x.IPreference <! UpdateState (selfNodeData,state,lastchangedtimestamp)
            
        | Route (x:NodeExtraData, counter: int) ->
            let mutable nodeToSend = getNextNode x 
            if nodeToSend.IsnotNull then
                nodeToSend.IPreference<! Route (x, counter + 1)
            else
                driverActorRef <! MessageRoutedSuccessfully counter
                
            
        // Update own state based on the state received
        | UpdateState (receivedData:NodeExtraData,receivedState:State, recievedtimestamp:DateTime) ->
            
            let mutable prefixlength = findPrefixLength (receivedData.NodeId.Key, selfNodeData.NodeId.Key )// send the hash value (nodeId)
            //Update the table
            for i = 0 to prefixlength do
                for j = 0 to b - 1 do   
                    if (j <> receivedData.NodeId.Key.[i] && (not state.RoutingTable.[i,j].IsnotNull) &&  (receivedState.RoutingTable.[i,j].IsnotNull))  then
                        state.RoutingTable.[i,j] <- receivedState.RoutingTable.[i,j]
                        lastchangedtimestamp  <- DateTime.Now

            if not state.RoutingTable.[0, receivedData.NodeId.Key.[0]].IsnotNull then
                state.RoutingTable.[0, receivedData.NodeId.Key.[0]] <- receivedData
            
            state.RoutingTable.[prefixlength, receivedData.NodeId.Key.[prefixlength]] <- receivedData
            
            // Updating the table from the shared smaller leaf set.
            for leaf in receivedState.SmallLeafSet do
                let l = findPrefixLength(hash, leaf.NodeId.Key)
                if leaf <> selfNodeData && not state.RoutingTable.[l, leaf.NodeId.Key.[l]].IsnotNull then
                    state.RoutingTable.[l, leaf.NodeId.Key.[l]] <- leaf

            // Updating the table from the shared larger leaf set.
            for leaf in receivedState.LargeLeafSet do
                let l = findPrefixLength(hash, leaf.NodeId.Key)
                if leaf <> selfNodeData && not state.RoutingTable.[l, leaf.NodeId.Key.[l]].IsnotNull then
                    state.RoutingTable.[l, leaf.NodeId.Key.[l]] <- leaf

            //Updated the leafset
            updateLeafSet(receivedState, receivedData) 

            // Send received message with timestamp to the sender 
            receivedData.IPreference <! Received (recievedtimestamp, selfNodeData)
        // Send a message to the neighbor A...Z that the messgage has been received
        | Received (recieved_timestamp:DateTime, sharedData) ->
            
            if recieved_timestamp.Equals lastchangedtimestamp then
                if status = InitializationSecondStep then
                    totalRecievedIn2ndStep <- totalRecievedIn2ndStep + 1
                    if totalSentIn2ndStep = totalRecievedIn2ndStep then
                        status <- InitializationDone
                        // Notify the supervisor in this case
                        driverActorRef <! NodeInitializationDone
                sharedData.IPreference <! FinishedSending
            elif recieved_timestamp < lastchangedtimestamp then  
                sharedData.IPreference <! UpdateState (selfNodeData,state,lastchangedtimestamp)
        // If all the neighbors have shared the state with the new neighbor X, then send new neighbor's state to all the neighbors
        | FinishedSending -> 
            
            if status = InitializationFirstStep then
                numberOfneighborsFinishedSendingData <- numberOfneighborsFinishedSendingData + 1
                if numberOfneighborsFinishedSendingData = numberofNeighboringActors then
                    status <- InitializationSecondStep
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

let random = Random()

printfn "Initializing nodes..."
for i = 0 to nodes - 1 do
    if (i = 0) then
        nodeArray.[i] <! Init null
    else 
        let next = random.Next(0, i)
        nodeArray.[i] <! Init nodeArray.[next]
        // printfn "Next: %A" next

    while (i + 1 <> numInitializationDone) do
        ()  

    // printfn "Initialized %A" (i+1)  
    // printfn "Initialized %A" numInitializationDone

printfn "Initializing done."

printfn "Sending messages..."
// Randomly send numRequest messages
let mutable randomValue = int 0
for i = 0 to nodes - 1 do
    for j = 1 to numRequests do
        randomValue <- random.Next(0, nodes)
        while(randomValue = i) do
            randomValue <- random.Next(0, nodes)
        nodeArray.[i] <! Route (getNodeId(nodeArray.[randomValue]), 0)

// Wait for all request to complete
while (nodes * numRequests <> totalRoutedMessages) do
    ()

printfn "Sent messages."

printfn "Average number of hops %A" (((double) totalRoutedMessagesCount)/((double)totalRoutedMessages))


Console.ReadLine() |> ignore
///////////////////////////Program////////////////////////////////////////
