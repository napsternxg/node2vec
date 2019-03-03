'''
Reference implementation of node2vec. 

Author: Aditya Grover

For more details, refer to the paper:
node2vec: Scalable Feature Learning for Networks
Aditya Grover and Jure Leskovec 
Knowledge Discovery and Data Mining (KDD), 2016
'''

import argparse
import numpy as np
import networkx as nx
import node2vec
from gensim.models import Word2Vec
import gzip
from tqdm import tqdm
from glob import glob
import logging

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
 


def parse_args():
	'''
	Parses the node2vec arguments.
	'''
	parser = argparse.ArgumentParser(description="Run node2vec.")

	parser.add_argument('--input', nargs='?', default='graph/karate.edgelist',
	                    help='Input graph path')

	parser.add_argument('--output', nargs='?', default='emb/karate.emb',
	                    help='Embeddings path')

	parser.add_argument('--dimensions', type=int, default=128,
	                    help='Number of dimensions. Default is 128.')

	parser.add_argument('--walk-length', type=int, default=80,
	                    help='Length of walk per source. Default is 80.')

	parser.add_argument('--num-walks', type=int, default=10,
	                    help='Number of walks per source. Default is 10.')

	parser.add_argument('--window-size', type=int, default=10,
                    	help='Context size for optimization. Default is 10.')

	parser.add_argument('--iter', default=1, type=int,
                      help='Number of epochs in SGD')

	parser.add_argument('--workers', type=int, default=8,
	                    help='Number of parallel workers. Default is 8.')

	parser.add_argument('--p', type=float, default=1,
	                    help='Return hyperparameter. Default is 1.')

	parser.add_argument('--q', type=float, default=1,
	                    help='Inout hyperparameter. Default is 1.')

	parser.add_argument('--weighted', dest='weighted', action='store_true',
	                    help='Boolean specifying (un)weighted. Default is unweighted.')
	parser.add_argument('--unweighted', dest='unweighted', action='store_false')
	parser.set_defaults(weighted=False)

	parser.add_argument('--directed', dest='directed', action='store_true',
	                    help='Graph is (un)directed. Default is undirected.')
	parser.add_argument('--undirected', dest='undirected', action='store_false')
	parser.add_argument('--cached-walks-path', type=str, default="cached_walks.txt",
	                    help='Path to cache walks.')
	parser.set_defaults(directed=False)

	return parser.parse_args()

def read_graph():
	'''
	Reads the input network in networkx.
	'''
	if args.weighted:
		G = nx.read_edgelist(args.input, nodetype=int, data=(('weight',float),), create_using=nx.DiGraph())
	else:
		G = nx.read_edgelist(args.input, nodetype=int, create_using=nx.DiGraph())
		for edge in G.edges():
			G[edge[0]][edge[1]]['weight'] = 1

	if not args.directed:
		G = G.to_undirected()

	return G

def walks2str_list(walks):
        for walk in tqdm(walks, desc="Process walks"):
                yield list(map(str, walk))


class Walks(object):
        def __init__(self, dirname):
                self.dirname = dirname

        def __iter__(self):
                for fname in glob("{}/part-*".format(self.dirname)):
                        with open(fname) as fp:
                            for line in fp:
                                    line = line.strip().split("\t")
                                    yield line

def load_walks(cache_path):
        walks = []
        open_fn = open
        if cache_path.endswith("gz"):
            open_fn = gzip.open
        with open_fn(cache_path) as fp:
                for line in tqdm(fp, desc="Reading walks"):
                        walk = line.strip().split("\t")
                        walks.append(walk)
        return walks

def save_walks(walks, cache_path):
        with open(cache_path, "w+") as fp:
                for walk in tqdm(walks, desc="Writing walks"):
                    print >> fp, "\t".join(walk)

def learn_embeddings(walks):
	'''
	Learn embeddings by optimizing the Skipgram objective using SGD.
	'''
	model = Word2Vec(walks, size=args.dimensions, window=args.window_size, min_count=0, sg=1, workers=args.workers, iter=args.iter)
	model.save(args.output)
	
	return

def main(args):
	'''
	Pipeline for representational learning for all nodes in a graph.
	'''
        print("Args: {}".format(args))
        try:
                #walks = load_walks(args.cached_walks_path)
                walks = Walks(args.cached_walks_path)
        except IOError as e:
                print("Found no cached walks at {}".format(args.cached_walks_path))
                print("Generating walks from graph.")
                nx_G = read_graph()
                G = node2vec.Graph(nx_G, args.directed, args.p, args.q, n_jobs=args.workers)
                print("Loaded graph: {}".format(G))
                print("Processing transition probabilities.")
                G.preprocess_transition_probs()
                print("Simulating walks.")
                walks = G.simulate_walks(args.num_walks, args.walk_length)
                walks = walks2str_list(walks)
                print("Saving walks at {}".format(args.cached_walks_path))
                save_walks(walks, args.cached_walks_path)
                print("Loading walks")
                walks = load_walks(args.cached_walks_path)
        print("Learning embeddings.")
	learn_embeddings(walks)

if __name__ == "__main__":
	args = parse_args()
	main(args)
