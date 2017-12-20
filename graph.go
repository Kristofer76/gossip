package main

import (
	"math/rand"
	"strconv"
)

type Node struct {
	id   int
	port int
}

type Graph map[Node][]Node

var basePort int

func newNode(a int) Node {
	return Node{
		id:   a,
		port: a + basePort,
	}
}

func (n Node) String() string {
	return strconv.Itoa(n.id)
}

func (n Node) Port() int {
	return n.port
}

func (g Graph) addEdge(a, b Node) {
	g[a] = append(g[a], b)
	g[b] = append(g[b], a)
}

func (g Graph) hasEdge(a, b Node) bool {
	if nodes, ok := g[a]; ok {
		for _, node := range nodes {
			if node == b {
				return true
			}
		}
	}
	return false
}

func (g Graph) Neighbors(id int) ([]Node, bool) {
	node := newNode(id)
	nodes, ok := g[node]
	return nodes, ok
}

func (g Graph) GetNode(id int) (Node, bool) {
	node := newNode(id)
	_, ok := g[node]
	return node, ok
}

// Generate generates connected random graph with n nodes.
// The degree of most part of vertices will be in interval [minDegree:maxDegree]
// some (not many) vertices can have more than maxDegree vertices.
//
// Port value is used as a base value for a port
// each generated node i (0 <= 0 <= n) will be associated
// with port p (p = i + port)
//
// Generate uses standard golang rand package,
// it is user's responsibility to init rand with a propper seed.
func Generate(n, minDegree, maxDegree, port int) Graph {
	if minDegree <= 0 || minDegree > maxDegree {
		panic("wrong minDegree or maxDegree value")
	}

	basePort = port
	g := make(Graph)

	degrees := make(map[int]int, n)
	for i := 0; i < n; i++ {
		degrees[i] = rand.Intn(maxDegree-minDegree) + minDegree
	}

	var nodes []int
	for len(degrees) > 1 {
		nodes = nodes[:0]
		for node := range degrees {
			nodes = append(nodes, node)
		}

		cur, nodes := nodes[0], nodes[1:]
		perm := rand.Perm(len(nodes))

		max := degrees[cur]
		if max > len(perm) {
			max = len(perm)
		}
		a := newNode(cur)
		for _, index := range perm[:max] {
			neigh := nodes[index]
			degrees[neigh]--
			if degrees[neigh] <= 0 {
				delete(degrees, neigh)
			}
			b := newNode(neigh)
			g.addEdge(a, b)
		}
		delete(degrees, cur)
	}

	for v := range degrees {
		a := newNode(v)
		if len(g[a]) > 0 {
			continue
		}
		b := v
		for b == v {
			b = rand.Intn(n)
		}
		g.addEdge(a, newNode(b))
	}

	perm := rand.Perm(n)
	for i := 1; i < n; i++ {
		a := perm[i-1]
		b := perm[i]
		na, nb := newNode(a), newNode(b)
		if !g.hasEdge(na, nb) {
			g.addEdge(na, nb)
		}
		degrees[a]--
		degrees[b]--
		if degrees[a] <= 0 {
			delete(degrees, a)
		}
		if degrees[b] <= 0 {
			delete(degrees, b)
		}
	}
	return g
}



