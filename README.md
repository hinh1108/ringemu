An emulation of Cassandra token ring distribution algorithm (SimpleStrategy)
to see how many distinct permutations of nodes the algorithm creates,
for a different number of nodes.

The program prints somethign like this:

cluster size: 4, groups: 4
cluster size: 5, groups: 10
cluster size: 6, groups: 20
cluster size: 7, groups: 35
cluster size: 8, groups: 56
cluster size: 9, groups: 84
cluster size: 10, groups: 120
cluster size: 11, groups: 165
cluster size: 12, groups: 220
cluster size: 13, groups: 285
cluster size: 14, groups: 360
cluster size: 15, groups: 445
...
cluster size: 99, groups: 18598
cluster size: 100, groups: 18849

Meaning, for a cluster with 4 nodes, each node having 256 vnodes, the ring
algorithm will select 4 distinct replica triplets responsible for
replication of vnodes.

The theoretical maximum is a binomial number C(NUM_NODES, REPLICATION_FACTOR),
and for the small clusters it is upheld, e.g. C(4,3) is 4, C(5,3) is 10 and
C(6,3) is 20.

But as we increase the number of nodes, the actual number of distinct
replicas is starting to lag behind the binomial coefficient thanks to
normal distribution of tokens on the ring, so for C(100, 3) it is 18849
rather than 161700.
