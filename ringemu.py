#!/usr/bin/env python3

# Cassandra replication factor
REPLICATION_FACTOR = 3
# Number of vnodes each physical node adds to the ring
NUM_TOKENS = 256
# Number of nodes to try in the simulation. We start from REPLICATION_FACTOR
# and run simulation up to this numer of nodes
NODE_COUNT_MAX = 100

class Replica:
    """A node in Cassandra ring. Uniquely identified by UUID"""
    uuid = None

    def gen_tokens(self):
        """Generate unique tokens for this replica. Skip a token and
        generate a new value if it is already in TokenMetadata"""
        pass

class ReplicaSet:
    """REPLICATION_FACTOR sized bag of replicas"""
    replicas = None
    
class TokenMetadata:
    """An ordered mapping of a token, which is an integer, to a
    ReplicaSet"""

    tokens = None

    def count_distinct_replicasets(self):
        return 1

class SimpleReplicationStrategy:
    """Given a new replica, generate tokens, add it to the cluster
        and assign replicas for all new ranges"""
    @staticmethod
    def update_token_metadata(replica, tm):
        pass


def main():
    """ Print a distribution of distinct replica triplets for clusters
        of different size"""

    for i in range(REPLICATION_FACTOR, NODE_COUNT_MAX):
        tm = TokenMetadata()
        for j in range(1, i):
            replica = Replica()
            SimpleReplicationStrategy.update_token_metadata(tm, replica)
        print("cluster size: {}, groups: {}".format(j,
                                                    tm.count_distinct_replicasets()))
                
if __name__ == '__main__':
    main()
