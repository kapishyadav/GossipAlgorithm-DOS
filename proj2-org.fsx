open System.Diagnostics
#r "packages/Akka.dll"
#r "packages/Akka.FSharp.dll"

open System
open Akka.FSharp
open Akka.Actor
open System.Linq;
open System.Collections.Generic

let system = ActorSystem.Create("Project")

//Define all the states of the actors
type ActorStates = 
    | Init of int * List<int> *bool
    | TransmitRumour
    | ListenRumour of decimal * decimal
    | RetransmitRumour of int



let command_params : string array = fsi.CommandLineArgs |> Array.tail
if command_params.Length <> 3 then
        failwith "Error: More than 3 arguments."
let mutable numNodes = command_params.[0] |> int 
let topologyName = command_params.[1] |> string
let alg = command_params.[2] |> string
let timestamp = Stopwatch()
timestamp.Start()
let actList =  new List<IActorRef>() 




//GOSSIP ALGORITHM

let GossipAlgorithm (inbox: Actor<ActorStates>) =
    //Actor number
    let mutable actorID = 0
    //No. of messages recieved
    let mutable tracker =1
    //Maximum messages receivable is 10
    let mutable maxLimit = false
    //construct a list of dead actors
    let mutable deadActors = new List<int>()
    //list of all neighbours in each topology
    let mutable neighboursList= new List<int>()
    
    
    let rec loop () = actor {
        let! msg = inbox.Receive ()
        match msg with
        | Init(ID,neighbourList,deadActor)  -> //initialize the actor
            actorID <- ID
            neighboursList <- neighbourList
            maxLimit <- deadActor
        | TransmitRumour  ->  //message transmission state
            let mutable  numNeighbours =   neighboursList.Count 
            let rand = Random()
            let mutable  numNeighbours =   neighboursList.Count 
            let mutable randNeighbourIndex  = rand.Next(0,numNeighbours)
            if numNeighbours = 0 then 
                numNeighbours <- numNodes
                randNeighbourIndex <- rand.Next(0,numNeighbours)
            else 
                randNeighbourIndex <- neighboursList.ElementAt(randNeighbourIndex)
            actList.ElementAt(randNeighbourIndex) <! ListenRumour( 0.0M,0.0M)
        | ListenRumour(test,test2) -> // receiving the transmission
            if maxLimit then
                inbox.Sender() <! RetransmitRumour actorID
            else 
                tracker<- tracker+1
                if tracker = 10 then
                    maxLimit <-true
                let rand = Random()
                let mutable  numNeighbours =   neighboursList.Count 
                let mutable randNeighbourIndex  = 0
                if numNeighbours = 0 then 
                    numNeighbours <- numNodes
                    randNeighbourIndex <- rand.Next(0,numNeighbours)
                    while randNeighbourIndex = actorID || deadActors.Contains(randNeighbourIndex)  do
                        randNeighbourIndex <- rand.Next(0,numNeighbours)
                else 
                    randNeighbourIndex <- rand.Next(0,numNeighbours)
                    randNeighbourIndex <- neighboursList.ElementAt(randNeighbourIndex)
                    while deadActors.Contains(randNeighbourIndex)  do
                        randNeighbourIndex <- rand.Next(0,numNeighbours)
                        randNeighbourIndex <- neighboursList.ElementAt(randNeighbourIndex)
                actList.ElementAt(randNeighbourIndex) <! ListenRumour(0.0M, 0.0M)
        | RetransmitRumour(deadNeighbour) ->  //retransmitting the message
            deadActors.Add(deadNeighbour)
            let rand = Random()
            let mutable  numNeighbours =   neighboursList.Count 
            let mutable randNeighbourIndex  = 0
            if numNeighbours = 0 then  // If full topology
                numNeighbours <- numNodes
                randNeighbourIndex <- rand.Next(0,numNeighbours)
                if deadActors.Count = numNodes-1 then //If actorList is full then terminate program
                    let timeTaken = timestamp.ElapsedMilliseconds
                    printfn "Converged"
                    printfn "Time Taken %d ms" timeTaken
                    system.Terminate() |> ignore

                // as long as the actors has neighbours alive, retransmit to a random neighbour
                while randNeighbourIndex = actorID || deadActors.Contains(randNeighbourIndex)  do    
                    randNeighbourIndex <- rand.Next(0,numNeighbours)
            else            //If any other topology
                // take a random neighbour
                randNeighbourIndex <- rand.Next(0,numNeighbours)
                randNeighbourIndex <- neighboursList.ElementAt(randNeighbourIndex)
                //check if all neighbours are dead
                if deadActors.Count = neighboursList.Count then
                    let timeTaken = timestamp.ElapsedMilliseconds
                    printfn "Converged"
                    printfn "Time Taken %d ms" timeTaken
                    system.Terminate() |> ignore
                //If all neighbours are not dead, do
                while deadActors.Contains(randNeighbourIndex)  do
                    randNeighbourIndex <- rand.Next(0,numNeighbours)
                    randNeighbourIndex <- neighboursList.ElementAt(randNeighbourIndex)
            actList.ElementAt(randNeighbourIndex) <! ListenRumour(0.0M, 0.0M)
        return! loop ()
    }
    loop ()



//  PUSHSUM ALGORITHM


let PushsumAlgorithm (inbox: Actor<ActorStates>) =
    //Define Pushsum actor props
    let mutable s = 0.0M
    let mutable w =1.0M
    // Ratio of s and w
    let mutable sbyw = 0.0M
    let mutable sbyw2 = 0.0M

    let mutable actorID = 0
    // Max no of msh transmittable tracker
    let mutable maxLimit = false
    //List of all actors as neighbours
    let mutable neighboursList= new List<int>()
    //List of all dead actors nearby
    let mutable deadActors = new List<int>()

    let rec loop () = actor {
        let! msg = inbox.Receive ()
        match msg with
        | Init(ID,neighbourList,deadActor)  ->
            s <- (decimal ID )+ 1.0M 
            actorID <- ID
            neighboursList <- neighbourList
            maxLimit <- deadActor
        | TransmitRumour  -> 
            let rand = Random()
            let mutable  numNeighbours =   neighboursList.Count 
            let mutable randNeighbourIndex  = rand.Next(0,numNeighbours)
            if numNeighbours = 0 then 
                numNeighbours <- numNodes
                randNeighbourIndex <- rand.Next(0,numNeighbours)
            else 
                randNeighbourIndex <- neighboursList.ElementAt(randNeighbourIndex)
            sbyw2 <- sbyw
            sbyw <- s/w
            //Keep half of s and w and transmit the rest
            s <-  s/2.0M
            w <- w/2.0M
            actList.ElementAt(randNeighbourIndex) <! ListenRumour(s,w)
        | ListenRumour(s_dash,w_dash) ->
            if maxLimit then
                inbox.Sender() <! RetransmitRumour actorID
            else 
                w<- w + w_dash
                s<- s + s_dash

                if Math.Abs((s/w)-sbyw) < 0.0000000001M && Math.Abs((s/w)-sbyw2) < 0.0000000001M && Math.Abs(sbyw2-sbyw) < 0.0000000001M then
                    maxLimit <-true
                let rand = Random()
                let mutable  numNeighbours =   neighboursList.Count 
                let mutable randNeighbourIndex  = 0
                if numNeighbours = 0 then 
                    numNeighbours <- numNodes
                    randNeighbourIndex <- rand.Next(0,numNeighbours)
                    while randNeighbourIndex = actorID || deadActors.Contains(randNeighbourIndex)  do
                        randNeighbourIndex <- rand.Next(0,numNeighbours)
                else 
                    randNeighbourIndex <- rand.Next(0,numNeighbours)
                    randNeighbourIndex <- neighboursList.ElementAt(randNeighbourIndex)
                    while deadActors.Contains(randNeighbourIndex)  do
                        randNeighbourIndex <- rand.Next(0,numNeighbours)
                        randNeighbourIndex <- neighboursList.ElementAt(randNeighbourIndex)
                sbyw2 <- sbyw
                sbyw <- s/w
                s <-  s/2.0M
                w <- w/2.0M
                actList.ElementAt(randNeighbourIndex) <! ListenRumour(s,w)
        | RetransmitRumour(deadNeighbour) ->
            deadActors.Add(deadNeighbour)
            let rand = Random()
            let mutable  numNeighbours =   neighboursList.Count 
            let mutable randNeighbourIndex  = 0
            if numNeighbours = 0 then 
                numNeighbours <- numNodes
                randNeighbourIndex <- rand.Next(0,numNeighbours)
                if deadActors.Count = numNodes-1 then
                    let timeTaken = timestamp.ElapsedMilliseconds
                    printfn "Converged"
                    printfn "Time Taken %d ms" timeTaken
                    system.Terminate() |> ignore
                while randNeighbourIndex = actorID || deadActors.Contains(randNeighbourIndex)  do
                    randNeighbourIndex <- rand.Next(0,numNeighbours)
            else
                randNeighbourIndex <- rand.Next(0,numNeighbours)
                randNeighbourIndex <- neighboursList.ElementAt(randNeighbourIndex)
                if deadActors.Count = neighboursList.Count then
                    let timeTaken = timestamp.ElapsedMilliseconds
                    printfn "Converged"
                    printfn "Time Taken %d ms" timeTaken
                    system.Terminate() |> ignore
                while deadActors.Contains(randNeighbourIndex)  do
                    randNeighbourIndex <- rand.Next(0,numNeighbours)
                    randNeighbourIndex <- neighboursList.ElementAt(randNeighbourIndex)
            actList.ElementAt(randNeighbourIndex) <! ListenRumour(s,w)
        return! loop ()
    }
    loop ()



let check2DActors n =
    let num = n &&& 0xF
    if (num > 9) then false
    else
        if ( num <> 2 && num <> 3 && num <> 5 && num <> 6 && num <> 7 && num <> 8 ) then
            let sqr = ((n |> double |> sqrt) + 0.5) |> floor|> int
            sqr*sqr = n
        else false


// If command line param - alg is not a valid one
if (alg <> "gossip") && alg <> "push-sum"  then
    failwith "Invalid Algorithm!!!" 









if topologyName = "full" then
    for i in 0 .. numNodes-1 do 
        let neighboursList  =  new List<int>() 
        let actor_Reference = if alg = "gossip" then  spawn system (string i) GossipAlgorithm else spawn system (string i) PushsumAlgorithm
        
        actor_Reference <! Init (i,neighboursList,false)
        actList.Add(actor_Reference)
elif topologyName = "line" then
    for i in 0 .. numNodes-1 do 
        let actor_Reference = if alg = "gossip" then  spawn system (string i) GossipAlgorithm else spawn system (string i) PushsumAlgorithm
        let neighboursList  =  new List<int>() 
        if(i=0) then 
            neighboursList.Add(1)
        elif (i=numNodes-1) then 
            neighboursList.Add(numNodes-2)
        else
            neighboursList.Add(i+1)
            neighboursList.Add(i-1)
        actor_Reference <! Init (i,neighboursList,false)
        actList.Add(actor_Reference)
elif topologyName = "2D" || topologyName= "imp2D" then
    while not (check2DActors numNodes) do 
        numNodes<- numNodes+1
    let sqrt = Math.Sqrt(numNodes|>float) |> int
    for i in 0 .. numNodes-1 do 
        let actor_Reference = if alg = "gossip" then  spawn system (string i) GossipAlgorithm else spawn system (string i) PushsumAlgorithm
        let neighboursList  =  new List<int>() 
        if i= 0 then 
            neighboursList.Add(1)
            neighboursList.Add(sqrt)
        elif i= sqrt-1 then
            neighboursList.Add(sqrt-2)
            neighboursList.Add(2*sqrt-1)
        elif i= numNodes-1 then
            neighboursList.Add(numNodes-2)
            neighboursList.Add(numNodes-sqrt-1)
        elif i= numNodes-sqrt then
            neighboursList.Add(numNodes-sqrt+1)
            neighboursList.Add(numNodes- 2*sqrt)
        elif i%sqrt=0 then
            neighboursList.Add(i+1)
            neighboursList.Add(i+sqrt)
            neighboursList.Add(i-sqrt)
        elif (i+1)%sqrt=0 then
            neighboursList.Add(i-1)
            neighboursList.Add(i+sqrt)
            neighboursList.Add(i-sqrt)
        elif i< sqrt then
            neighboursList.Add(i-1)
            neighboursList.Add(i+1)
            neighboursList.Add(i+sqrt)
        elif i > numNodes - sqrt then
            neighboursList.Add(i-1)
            neighboursList.Add(i+1)
            neighboursList.Add(i-sqrt)
        else 
            neighboursList.Add(i+1) 
            neighboursList.Add(i-1) 
            neighboursList.Add(i+sqrt) 
            neighboursList.Add(i-sqrt) 
        if topologyName ="imp2D" then
            let rand =  Random()
            let mutable randNeighbourIndex = rand.Next(0,numNodes)
            while(neighboursList.Contains(randNeighbourIndex) || randNeighbourIndex = i) do 
                randNeighbourIndex <- rand.Next(0,numNodes)
            neighboursList.Add(randNeighbourIndex)
        actor_Reference <! Init (i,neighboursList,false)
        actList.Add(actor_Reference)



let rand =  Random()
let mutable randNumber = rand.Next(0,numNodes)
actList.ElementAt(randNumber) <! TransmitRumour
Console.ReadLine()

