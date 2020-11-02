#r "nuget: Akka.FSharp" 
#r "nuget: Akka.TestKit" 

open System
open Akka.Actor

open Akka.FSharp
open System.Security.Cryptography
open System.Text
///////////////////////////Initialization////////////////////////////////////////

type Message = 
    | InitializeKeyValue of String
    | Join
    | Route

type NodeData ={
    Key: bigint
    Value: bigint 
    
}
type NodeExtraData= {
    NodeId: NodeData
    IPreference: IActorRef

}

type State = {
    SmallLeafSet:NodeExtraData [] 
    LargeLeafSet:NodeExtraData []
    RoutingTable: NodeExtraData [,]
}

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
    let state:State ={
        SmallLeafSet = Array.create sizeofhalfleafset null
        LargeLeafSet = Array.create sizeofhalfleafset null
        RoutingTable = Array2D.create numberOfRows numberOfCols null

    }
    

    let rec loop()= actor{
        let! message = mailbox.Receive();
        
        match message with
        | InitializeKeyValue key ->
             
        
        
        return! loop()
    }            
    loop()


///////////////////////////Worker Actor////////////////////////////////////////

///////////////////////////Program////////////////////////////////////////

nodeArray <- Array.zeroCreate (nodes + 1)

let md5 (data : byte array) : string =
    let md5 = MD5.Create()
    (StringBuilder(), md5.ComputeHash(data))
    ||> Array.fold (fun sb b -> sb.Append(b.ToString("x2")))
    |> string

for x in [0..nodes] do
    let actorkey : string = "actor" + string(x) 
    let actorRef = spawn system (actorkey) Worker
    let keyHash: string = md5 "actorkey"B
    let value: string = "Actor invoked " + string(x)
    nodeArray.[x] <- actorRef 
    nodeArray.[x] <! InitializeKeyValue (keyHash,value)

Console.ReadLine() |> ignore
///////////////////////////Program////////////////////////////////////////
