import numpy as np
import networkx as nx
import random
from tqdm import tqdm

class Graph():
	def __init__(self, nx_G, is_directed, p, q, n_jobs=1):
		self.G = nx_G
		self.is_directed = is_directed
		self.p = p
		self.q = q
                self.n_jobs = n_jobs

        def __repr__(self):
            return "<G(nodes={}, edges={}), is_directed={}, p={}, q=q>".format(
                    self.G.number_of_nodes(), self.G.size(),
                    self.is_directed,
                    self.p, self.q
                    )

	def simulate_walks(self, num_walks, walk_length):
		'''
		Repeatedly simulate random walks from each node.
		'''
		G = self.G
		nodes = list(G.nodes())
		print 'Walk iteration:'
		for walk_iter in tqdm(range(num_walks), desc="Walks"):
			print str(walk_iter+1), '/', str(num_walks)
			random.shuffle(nodes)
                        new_walks = get_walks_for_nodes(
                                G, self.alias_nodes, self.alias_edges, nodes, walk_length=walk_length
                                )
                        for walk in new_walks:
                                yield walk

	def get_alias_edge(self, src, dst):
		'''
		Get the alias edge setup lists for a given edge.
		'''
		G = self.G
		p = self.p
		q = self.q

		unnormalized_probs = []
		for dst_nbr in sorted(G.neighbors(dst)):
			if dst_nbr == src:
				unnormalized_probs.append(G[dst][dst_nbr]['weight']/p)
			elif G.has_edge(dst_nbr, src):
				unnormalized_probs.append(G[dst][dst_nbr]['weight'])
			else:
				unnormalized_probs.append(G[dst][dst_nbr]['weight']/q)
		norm_const = sum(unnormalized_probs)
		normalized_probs =  [float(u_prob)/norm_const for u_prob in unnormalized_probs]

		return alias_setup(normalized_probs)

	def preprocess_transition_probs(self):
		'''
		Preprocessing of transition probabilities for guiding the random walks.
		'''
		G = self.G
		is_directed = self.is_directed

                alias_nodes = generate_alias_nodes(self.n_jobs, (
                    (node, 
                    [G[node][nbr]['weight'] for nbr in sorted(G.neighbors(node))])
                    for node in G.nodes()
                    ))
                        

		alias_edges = {}
		triads = {}

		if is_directed:
			for edge in tqdm(G.edges(), desc="Transition edges"):
				alias_edges[edge] = self.get_alias_edge(edge[0], edge[1])
		else:
			for edge in G.edges():
				alias_edges[edge] = self.get_alias_edge(edge[0], edge[1])
				alias_edges[(edge[1], edge[0])] = self.get_alias_edge(edge[1], edge[0])

		self.alias_nodes = alias_nodes
		self.alias_edges = alias_edges

		return


def alias_setup(probs):
	'''
	Compute utility lists for non-uniform sampling from discrete distributions.
	Refer to https://hips.seas.harvard.edu/blog/2013/03/03/the-alias-method-efficient-sampling-with-many-discrete-outcomes/
	for details
	'''
	K = len(probs)
	q = np.zeros(K)
	J = np.zeros(K, dtype=np.int)

	smaller = []
	larger = []
	for kk, prob in enumerate(probs):
	    q[kk] = K*prob
	    if q[kk] < 1.0:
	        smaller.append(kk)
	    else:
	        larger.append(kk)

	while len(smaller) > 0 and len(larger) > 0:
	    small = smaller.pop()
	    large = larger.pop()

	    J[small] = large
	    q[large] = q[large] + q[small] - 1.0
	    if q[large] < 1.0:
	        smaller.append(large)
	    else:
	        larger.append(large)

	return J, q

def alias_draw(J, q):
	'''
	Draw sample from a non-uniform discrete distribution using alias sampling.
	'''
	K = len(J)

	kk = int(np.floor(np.random.rand()*K))
	if np.random.rand() < q[kk]:
	    return kk
	else:
	    return J[kk]

def get_alias_nodes(node, unnormalized_probs):
        norm_const = sum(unnormalized_probs)
        normalized_probs =  [float(u_prob)/norm_const for u_prob in unnormalized_probs]
        alias_nodes = alias_setup(normalized_probs)
        return (node, alias_nodes)


def generate_alias_nodes(n_jobs, node_neighbor_weights_iterator):
        alias_nodes = []
        for node, neighbor_weights in tqdm(node_neighbor_weights_iterator, desc="Alias nodes"):
                alias_nodes.append(get_alias_nodes(node, neighbor_weights))
        return dict(alias_nodes)

def node2vec_walk(G, alias_nodes, alias_edges, walk_length, start_node):
        '''
        Simulate a random walk starting from start node.
        '''
        walk = [start_node]

        while len(walk) < walk_length:
                cur = walk[-1]
                cur_nbrs = sorted(G.neighbors(cur))
                if len(cur_nbrs) > 0:
                        if len(walk) == 1:
                                walk.append(cur_nbrs[alias_draw(alias_nodes[cur][0], alias_nodes[cur][1])])
                        else:
                                prev = walk[-2]
                                next = cur_nbrs[alias_draw(alias_edges[(prev, cur)][0], 
                                        alias_edges[(prev, cur)][1])]
                                walk.append(next)
                else:
                        break

        return walk

def get_walks_for_nodes(G, alias_nodes, alias_edges, nodes, walk_length):
    for node in tqdm(nodes, desc="Node walks"):
            yield node2vec_walk(G, alias_nodes, alias_edges, walk_length, node)
