package main

import (
	"fmt"
	"time"

	"github.com/rhino1998/hypercube/node"
)

func main() {
	node.NewLocal(4999, 6, 8, "")
	fmt.Println(4999)
	seed := "localhost:4999"
	for i := 0; i < 22; i++ {
		v, err := node.NewLocal(i+5000, 6, 8, seed)
		fmt.Println(err)
		fmt.Println(v.Node.ID)
	}
	q, err := node.NewNeighbor(seed)
	time.Sleep(2 * time.Second)
	neighbors, err := q.GetNeighbors()
	for _, neighbor := range neighbors {
		if neighbor == nil {
			continue
		}
		neighbor.Connect()
		fmt.Println("connected", neighbor.ID())
		neighborsNeighbors, _ := neighbor.GetNeighbors()
		fmt.Println("neighbor: ", neighbor.ID())
		for _, neighborsNeighbor := range neighborsNeighbors {
			fmt.Print(neighborsNeighbor.ID(), " ")
		}
		fmt.Println()
	}
	for i := 0; i < 128; i++ {
		q.Set(string(i), []byte(fmt.Sprint(i*8)))
	}
	//fmt.Println(err)
	for i := 0; i < 128; i++ {
		//q.Set(string(i), []byte(fmt.Sprint(i*8)))
		time.Sleep(10 * time.Millisecond)
		val, err := q.Get(string(i))
		fmt.Println(string(val), err, i*8)
	}
	time.Sleep(2)
	for i := 0; i < 22; i++ {
		fmt.Print("making", i+5100)
		v, _ := node.NewLocal(i+5100, 6, 8, seed)
		fmt.Println(v.Node.ID)
	}
	time.Sleep(2)
	fmt.Println("Done Filling")
	start := time.Now()
	val, err := q.Get(string(45))
	fmt.Println(string(val), err, time.Since(start))

}
