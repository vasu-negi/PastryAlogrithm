open System.Security.Cryptography
open System.Text

let md5 (data : byte array) : string =
    let md5 = MD5.Create()
    (StringBuilder(), md5.ComputeHash(data))
    ||> Array.fold (fun sb b -> sb.Append(b.ToString("x2")))
    |> string

let hash1 = md5 "1"B
let hash2 = md5 "1"B
printfn "%A" hash
printfn "%A" hash21