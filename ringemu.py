#!/usr/bin/env python3

import uuid
import random
import bisect
import pprint

# Cassandra replication factor
REPLICATION_FACTOR = 3
# Number of vnodes each physical node adds to the ring
NUM_TOKENS = 256
# Number of nodes to try in the simulation. We start from
# REPLICATION_FACTOR and run simulation up to this numer of nodes
NODE_COUNT_MAX = 1000
# Each token value is within range 0..TOKEN_MAX
TOKEN_MAX = 1000000000

TWEAK1 = True 

class TokenMetadata:
    """An ordered mapping of a token, which is an integer, to a
    ReplicaSet"""

    nodes = []
    tokens = {}
    primaries = {}
    sorted_tokens = None

    def __init__(self, replication_factor):
        """Create replicas and add seeds for replication_factor nodes"""

        # First create enough replicas so that we have enough peers
        for i in range(0, replication_factor):
            replica = Replica()
            replica.gen_tokens(self)
            self.nodes.append(replica)
        self.sorted_tokens = sorted(self.tokens.keys())
        # Create replicasets from the initial set of replicas.
        self.replicasets = set()
        self.set_peers()

    def upper_bound(self, token):
        assert(len(self.tokens.keys()))

        upper = bisect.bisect_right(self.sorted_tokens, token);
        if upper >= len(self.sorted_tokens):
            upper = 0
        token = self.sorted_tokens[upper]
        return self.primaries[token], token

    def count_distinct_replicasets(self):
#        for node in self.nodes:
#            peers = set({node.uuid})
#            for token in node.tokens:
#                for peer in self.tokens[token].replicas:
#                    peers.add(peer.uuid)
#            print("Node {} has {} peers".format(node.uuid, len(peers)))
        return len(self.replicasets)

    def register_token(self, token, primary):
        """Add token to the ordered set of tokens and set the passed
        replica as the primar token. Creates a replicaset for the token
        with no peers, the peers are set afterwards with set_peers() call,
        to make bootstrap possible"""

        self.primaries[token]  = primary;
        self.tokens[token] = ReplicaSet(token);

    def gen_tokens(self, replica):
        replica.gen_tokens(self)
        self.nodes.append(replica)
        self.sorted_tokens = sorted(self.tokens.keys())

    def set_peers(self):
        """Set replicaset peers. We can begin setting peers only after
            we registered all primaries"""
        self.replicasets = set()
        for token in self.tokens.keys():
            self.tokens[token].set_peers(self)
            self.replicasets.add(self.tokens[token].str_uuids)


class Replica:
    """A node in Cassandra ring. Uniquely identified by UUID"""
    uuid = None
    tokens = None

    def __init__(self):
        self.uuid = uuid.uuid4()
        self.tokens = []

    def gen_tokens(self, token_metadata):
        """Generate unique tokens for this replica. Re-generate
         tokens which are already on the ring, to avoid duplicates"""

        while len(self.tokens) < NUM_TOKENS:
            token = random.randint(0, TOKEN_MAX)
            if token in token_metadata.tokens:
                continue
            self.tokens.append(token)
            token_metadata.register_token(token, self)

class ReplicaSet:
    """REPLICATION_FACTOR sized bag of replicas"""
    token = None
    replicas = None
    uuids = None

    def __init__(self, token):
        self.token = token
        self.replicas = []
        uuids = set()

    def set_peers(self, token_metadata):
        token = self.token
        self.replicas = [ token_metadata.primaries[token] ]
        self.uuids = set(x.uuid for x in self.replicas)
        while len(self.uuids) < REPLICATION_FACTOR:
            replica, token = token_metadata.upper_bound(token)
            if replica.uuid not in self.uuids:
                self.uuids.add(replica.uuid)
                self.replicas.append(replica)
                if TWEAK1 and len(token_metadata.tokens[token].replicas) > 1 and len(self.uuids) < REPLICATION_FACTOR:
                    # Add ex-secondary as the tertiary right away
                    secondary = token_metadata.tokens[token].replicas[1]
                    if secondary.uuid not in self.uuids:
                        self.uuids.add(secondary.uuid)
                        self.replicas.append(secondary)
        self.str_uuids = ""
        for uuid in self.uuids:
            self.str_uuids = self.str_uuids + " " + str(uuid)
                    

    def __hash__(self):
        """Ignore the primary replica in identity functions. Replicasets
           with the same set of nodes but a different primary
           are considered identical"""

        return hash(self.str_uuids) #x for x in self.uuids)

    def __eq__(self, other):
        if not isinstance(other, ReplicaSet):
            return NotImplemented
        return self.uuids == other.uuids

    def __str__(self):
        return str(self.uuids)

def main():
    """ Print a distribution of distinct replica triplets for clusters
        of different size"""

    print("Replication factor {}, num_tokens = {}, counting up to {} nodes"
          .format(REPLICATION_FACTOR, NUM_TOKENS, NODE_COUNT_MAX))

    tm = TokenMetadata(REPLICATION_FACTOR)
    for i in range(REPLICATION_FACTOR + 1, NODE_COUNT_MAX + 1):
        replica = Replica()
        tm.gen_tokens(replica)
        tm.set_peers()
        print("cluster size: {}, groups: {}".format(i,
                                                    tm.count_distinct_replicasets()))

if __name__ == '__main__':
    main()
