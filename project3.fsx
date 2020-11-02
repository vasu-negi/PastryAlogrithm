#r "nuget: Akka.FSharp" 
#r "nuget: Akka.TestKit" 

open System
open Akka.Actor
open Akka.Configuration
open Akka.FSharp
open System.Diagnostics
open System.Collections.Generic
///////////////////////////Initialization////////////////////////////////////////
type GossipMessageTypes =
    | Initailize of IActorRef []
    | InitializeVariables of int
    | StartGossip of String
    | ReportMsgRecvd of String
    | StartPushSum of Double
    | ComputePushSum of Double * Double * Double
    | Result of Double * Double
    | Time of int
    | TotalNodes of int
    | ActivateWorker 
    | CallWorker
    | AddNeighbors
    

type 

let mutable nodes =
    int (string (fsi.CommandLineArgs.GetValue 1))

let numRequests = string (fsi.CommandLineArgs.GetValue 2)

let timer = Diagnostics.Stopwatch()
let system = ActorSystem.Create("System")
let mutable actualNumOfNodes = nodes |> float



let mutable  nodeArray = [||]
///////////////////////////Initialization////////////////////////////////////////

///////////////////////////Supervisor Actor////////////////////////////////////////
let Supervisor(mailbox: Actor<_>) =
    
    let mutable count = 0
    let mutable start = 0
    let mutable totalNodes = 0

    let rec loop()= actor{
        let! msg = mailbox.Receive();
        match msg with 
        

        | ReportMsgRecvd _ -> 
            let ending = DateTime.Now.TimeOfDay.Milliseconds
            count <- count + 1
            if count = totalNodes then
                timer.Stop()
                printfn "Time for convergence: %f ms" timer.Elapsed.TotalMilliseconds
                Environment.Exit(0)
        | Result (sum, weight) ->
            count <- count + 1
            if count = totalNodes then
                timer.Stop()
                
                printfn "Time for convergence: %f ms" timer.Elapsed.TotalMilliseconds
                Environment.Exit(0)
        | Time strtTime -> start <- strtTime
        | TotalNodes n -> totalNodes <- n
        | _ -> ()

        return! loop()
    }            
    loop()
///////////////////////////Supervisor Actor////////////////////////////////////////

let supervisor = spawn system "Supervisor" Supervisor
let dictionary = new Dictionary<IActorRef, bool>()

///////////////////////////Worker Actor////////////////////////////////////////
let Worker(mailbox: Actor<_>) =
    

    let mutable leafSet:IActorRef [] = [||]
    let mutable routingTable:IActorRef [] = [||]

    let rec loop()= actor{
        let! message = mailbox.Receive();
        
        match message with 
        | 

        
       
        | _ -> ()
        return! loop()
    }            
    loop()





//let GossipActor = spawn system "ActorWorker" ActorWorker

///////////////////////////Worker Actor////////////////////////////////////////

///////////////////////////Program////////////////////////////////////////

nodeArray <- Array.zeroCreate (nodes + 1)
for x in [0..nodes] do
    let key: string = "actor" + string(x) 
    let actorRef = spawn system (key) Worker
    nodeArray.[x] <- actorRef 





Console.ReadLine() |> ignore
///////////////////////////Program////////////////////////////////////////
