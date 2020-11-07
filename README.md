#Build Process:

• unzip the compressed file using unzip filename.zip

• dotnet fsi --langversion:preview project3.fsx numNodes numRequests 

to run script where numNodes is the number of nodes you want to run Pastry for. numRequests is the amount of request each peer has to make.

#What is Working:

Node network

• Nodes are added to the network as they are received

• b=4

• Key size = 128

• Each leaf set is of size L = 16 with 8 nodeids having a smaller value and 8 larger value than the nodeid

• We tested the network for varying amount of nodes and number of requests and we found that average number of hops comes around logN/log 16.

#What is the largest network you managed to deal with for each type of topology and algorithm:

The largest network that we have managed to solve is 35k nodes/peers with each node making 10 request. 
