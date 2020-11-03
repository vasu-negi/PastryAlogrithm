open System.Security.Cryptography
open System.Text

let md5 (data : byte array) =
    let md5 = MD5.Create()
    md5.ComputeHash(data)

let hash1 = md5 "1"B
let hash2 = md5 "1"B
printfn "%A" hash
