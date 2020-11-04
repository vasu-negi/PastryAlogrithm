open System
let rng = Random()
let Shuffle (org:_[]) = 
    let arr = Array.copy org
    let max = (arr.Length - 1)
    let randomSwap (arr:_[]) i =
        let pos = Random().Next(max)
        let tmp = arr.[pos]
        arr.[pos] <- arr.[i]
        arr.[i] <- tmp
        arr
    [|0..max|] |> Array.fold randomSwap arr
let bigintToDigits b source =
    let rec loop (b : int) num digits =
        let (quotient, remainder) = bigint.DivRem(num, bigint b)
        match quotient with
        | zero when zero = 0I -> int remainder :: digits
        | _ -> loop b quotient (int remainder :: digits)
    loop b source []

let digitsToString length source =
    let base4String = source |> List.map (fun (x : int) -> x.ToString("X").ToLowerInvariant()) |> String.concat ""
    let zeroLength = length - base4String.Length
    String.replicate zeroLength "0" + base4String



let getBase4String (source:int) (length:int) =
    let bigintToBase4 = bigintToDigits 4
    bigint(source) |> bigintToBase4 |>  digitsToString length
    
let getPrefixLen (s1:string) (s2:string) = 
    let mutable j = 0
    while j < s1.Length && s1.[j] = s2.[j] do
        j <- j+1
    j
let numNodes = 1000 
let baseVal = int(ceil (log10(float(numNodes))/log10(float(4))))
let nodeIDSpace = int(float(4) ** float(baseVal))
let myID = 103

printfn "%A" (getBase4String myID baseVal )

let mutable x= Array2D.create 10 10 0
printfn "%A" x.GetLength









