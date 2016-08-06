package node

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"strconv"
	"sync"

	"github.com/rhino1998/hypercube/common"
	"github.com/rhino1998/util"
)

/*For future reference, optimal location refers to the node closest to the hash of the key to the data by XOR
All RPC methods return valued by modifying the contents of a pointer passed as a argument. It is a little unconventional,
but it is a characteristic of Go's rpc package.
All RPC methods are also locked by a system of locks prioritizing internal state of the node before external requests are considered
RPC method intended to be available to external actors are also locked by a semaphor that limits the number of concurrent accesses to a set number
This is not to maintain threadsafety, but rather to limit the number of concurrent threads to a sensible number
*/

//Local implements an RPCNode
type Local struct {
	Node
	data         map[string][]byte
	datalock     *sync.RWMutex
	neighbors    []RPCNodeProxy
	neighborlock *sync.RWMutex
	mutex        *common.IntExtLock
	sem          *util.Semaphore
}

//NewLocal makes new local node at a specified port in a dht of dim dimensions. Dim is overridden by the seednode if seenode is defined
func NewLocal(port int, dims uint, maxrequests int, seednode *Node) (*Local, error) {
	var id uint64

	ip := getip()

	local := &Local{
		Node: Node{
			ID:   0,
			IP:   ip,
			Port: port,
		},
		data:         make(map[string][]byte),
		datalock:     &sync.RWMutex{},
		neighbors:    make([]RPCNodeProxy, dims),
		neighborlock: &sync.RWMutex{},
		mutex:        common.NewIntExtLock(),
		sem:          util.NewSemaphore(maxrequests),
	}
	local.startRPC()

	if seednode != nil {
		neighbor, err := NewNeighbor(fmt.Sprintf("%v:%v", seednode.IP, seednode.Port))
		dims, err = neighbor.GetDims()
		id, err = neighbor.AssistBootstrap(fmt.Sprintf("%v:%v", ip, port))
		if err != nil {
			return nil, err
		}
		local.neighbors = make([]RPCNodeProxy, dims)
		local.Node.ID = id
		local.mutex.RIntLock()
		local.updateNeighbors(neighbor)
		local.mutex.RIntUnlock()
		go local.getNeighbors(neighbor)
		if err != nil {
			return nil, err
		}
	}
	//TBI: id deciding, initial greeting
	return local, nil
}

//startRPC starts an rpc server at the local node
//Local Method
func (local *Local) startRPC() {
	server := rpc.NewServer()
	server.RegisterName("RPCNode", local)
	l, e := net.Listen("tcp", fmt.Sprintf(":%v", local.Node.Port))
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				log.Fatal(err)
			}

			go server.ServeConn(conn)
		}
	}()
}

//updateStore relocates data to its proper location
//Local Method
func (local *Local) updateStore(neighbor RPCNodeProxy) {
	local.mutex.RIntLock()
	defer local.mutex.RIntUnlock()
	for key := range local.data {
		if !isCloser(local.Node.ID, neighbor.ID(), keyToID(key, len(local.neighbors))) {
			continue
		}
		fmt.Println("store", neighbor.ID(), local.ID)
		local.datalock.Lock()
		err := neighbor.Relocate(key, local.data[key])
		fmt.Println("UPDATED")
		if err != nil {
			continue
		}
		delete(local.data, key)
		local.datalock.Unlock()
	}

}

//getNeighbors initially populates the node with a set of ideally optimal neighbors
//Local Method
func (local *Local) getNeighbors(neighbor RPCNodeProxy) {
	//time.Sleep(2 * time.Second)
	if neighbor == nil {
		return
	}
	local.mutex.RIntLock()
	defer local.mutex.RIntUnlock()
	for i := 0; i < len(local.neighbors); i++ {
		newNeighborNode, err := neighbor.FindNeighbor(&local.Node, local.Node.ID^(1<<uint(i)))
		newNeighbor, err := NewNeighbor(&newNeighborNode)
		if err == nil {
			local.updateNeighbors(newNeighbor)
		}
	}
}

//updateNeighbors adds and initalizes new neighbors and relocates data as necessary
//Local Method
func (local *Local) updateNeighbors(newNeighbor RPCNodeProxy) error {
	if newNeighbor == nil || (newNeighbor.IP() == local.Node.IP && newNeighbor.Port() == local.Node.Port) {
		return ErrorNilNeighbor
	}
	//fmt.Println(newNeighbor.ID())
	for i, neighbor := range local.neighbors {
		if neighbor == nil {
			err := newNeighbor.Connect()
			if err != nil {
				return err
			}
			local.updateStore(newNeighbor)

			local.neighborlock.Lock()
			local.neighbors[i] = newNeighbor
			local.neighborlock.Unlock()

			return nil
		}
		if neighbor.ID() == newNeighbor.ID() {
			break
		}
		//fmt.Println(neighbor.ID(), newNeighbor.ID(), local.Node.ID^(1<<uint(i)))
		if isCloser(neighbor.ID(), newNeighbor.ID(), local.Node.ID^(1<<uint(i))) {
			oldNeighbor := local.neighbors[i]
			err := newNeighbor.Connect()
			if err != nil {
				return err
			}
			local.updateStore(newNeighbor)

			local.neighborlock.Lock()
			local.neighbors[i] = newNeighbor
			local.neighborlock.Unlock()

			local.updateNeighbors(oldNeighbor)
			return nil
		}
	}
	//fmt.Println("closing", newNeighbor.ID(), newNeighbor.Port(), local.Node.Port)
	newNeighbor.Close()
	return nil
}

//findID returns the neighbor closest to the given deciding
//Local Method
func (local *Local) findID(id uint64) (RPCNodeProxy, error) {
	var closest RPCNodeProxy
	closestID := local.Node.ID
	for _, neighbor := range local.neighbors {
		if neighbor == nil {
			continue
		}
		if isCloser(closestID, neighbor.ID(), id) {
			closest = neighbor
			closestID = neighbor.ID()
		}
	}
	if closest != nil && closest.ID() == local.Node.ID {
		return nil, nil
	}
	return closest, nil
}

//closestToKey returns the neighbor closest to the key given
//Local Method
func (local *Local) closestToKey(key string) (RPCNodeProxy, error) {
	return local.findID(keyToID(key, len(local.neighbors)))
}

//Get value from key
//RPC Method
func (local *Local) Get(key *string, data *[]byte) error {

	local.mutex.RExtLock()
	local.sem.Lock()
	defer local.sem.Unlock()
	defer local.mutex.RExtUnlock()

	closest, err := local.closestToKey(*key)

	if err != nil {
		return err
	}

	if closest == nil {
		local.datalock.RLock()
		val, found := local.data[*key]
		local.datalock.RUnlock()
		if !found {
			return NewKeyNotFound(*key, local.Node.ID)
		}
		*data = val
		//fmt.Println(*data)
		fmt.Println(local.Node.ID, keyToID(*key, len(local.neighbors)))
		return nil
	}

	local.sem.Lock()
	defer local.sem.Unlock()
	*data, err = closest.Get(*key)
	return err
}

//Relocate moves data/key to the optimal location
//
func (local *Local) Relocate(item *common.Item, _ *struct{}) error {
	local.mutex.RIntLock()
	defer local.mutex.RIntUnlock()
	closest, err := local.closestToKey(item.Key)

	if err != nil {
		return err
	}

	if closest == nil {
		//fmt.Println(item.Data)
		local.datalock.Lock()
		local.data[item.Key] = item.Data
		local.datalock.Unlock()
		return nil
	}

	return closest.Set(item.Key, item.Data)
}

//Set a value in dht
//RPC Method
func (local *Local) Set(item *common.Item, _ *struct{}) error {
	local.mutex.RExtLock()
	local.sem.Lock()
	defer local.sem.Unlock()
	defer local.mutex.RExtUnlock()
	closest, err := local.closestToKey(item.Key)

	if err != nil {
		return err
	}

	if closest == nil {
		//fmt.Println(item.Data)
		local.datalock.Lock()
		local.data[item.Key] = item.Data
		local.datalock.Unlock()
		return nil
	}

	return closest.Set(item.Key, item.Data)
}

func (local *Local) Del(key *string, _ *struct{}) error {

	local.mutex.RExtLock()
	local.sem.Lock()
	defer local.sem.Unlock()
	defer local.mutex.RExtUnlock()

	closest, err := local.closestToKey(*key)

	if err != nil {
		return err
	}

	if closest == nil {
		local.datalock.Lock()
		delete(local.data, *key)
		local.datalock.Unlock()
		return nil
	}

	//Calls RPC and returns err (if any)
	return closest.Del(*key)
}

//Pong returns a ping from a remote node
//RPC Method
func (local *Local) Pong(*struct{}, *struct{}) error {
	return nil
}

//Info returns the local node attributes
//RPC Method
func (local *Local) Info(_ *struct{}, node *Node) error {
	*node = local.Node
	return nil
}

//GetNeighbors returns the neighbors of the local node
//RPC Method
func (local *Local) GetNeighbors(_ *struct{}, neighbors *[]RPCNodeProxy) error {
	local.mutex.RExtLock()
	fmt.Println("GetNeighbors")
	local.sem.Lock()
	defer local.sem.Unlock()
	defer local.mutex.RExtUnlock()

	numNeighbors := 0
	for _, neighbor := range local.neighbors {
		if neighbor != nil {
			numNeighbors++
		}
	}
	*neighbors = make([]RPCNodeProxy, numNeighbors)
	for _, neighbor := range local.neighbors {
		if neighbor != nil {
			numNeighbors--
			(*neighbors)[numNeighbors] = neighbor
		}
	}
	return nil
}

//AssistBootstrap attempts to assign the next id to a node
//RPC Method
func (local *Local) AssistBootstrap(addr *string, id *uint64) error {
	next := nextNeighbor(local.Node.ID) - 1
	optimalID := uint64(local.Node.ID ^ (1 << (next)))

	//If the slot for a neighbor differing by the {next} bit is empty or occupied by a sub-optimal neighbor
	if local.neighbors[next] == nil || local.neighbors[next].ID() != optimalID {

		local.mutex.RIntLock()
		defer local.mutex.RIntUnlock()

		//Assign the optimal id so that it is returned to the sender
		*id = optimalID
		ip, port, err := net.SplitHostPort(*addr)
		portNum, err := strconv.Atoi(port)

		//Initialize the new neighbor
		newNeighbor, err := NewNeighbor(&Node{
			IP:   ip,
			Port: portNum,
			ID:   *id,
		})
		//Make sure everything worked, otherwise convey failure
		if err != nil {
			return err
		}

		//Save old contents, place new neighbor, and update neighbors
		oldNeighbor := local.neighbors[next]
		local.neighbors[next] = newNeighbor
		if oldNeighbor != nil {
			local.updateNeighbors(oldNeighbor)
		}
		return nil
	}

	var err error
	local.sem.Lock()
	*id, err = local.neighbors[next].AssistBootstrap(*addr)
	local.sem.Unlock()
	return err
}

//FindNeighbor attempts to find the most optimal neighhbor available
//RPCMethod
func (local *Local) FindNeighbor(info *FindMsg, neighborNode *Node) (err error) {
	local.sem.Lock()
	defer local.sem.Unlock()

	neighbor, err := local.findID(info.Target)
	if err != nil {
		return err
	}
	if neighbor == nil {
		local.mutex.RIntLock()
		defer local.mutex.RIntUnlock()
		newNeighbor, err := NewNeighbor(info.Node)
		if err == nil {
			local.updateNeighbors(newNeighbor)
		}

		*neighborNode = local.Node
		return nil
	}
	*neighborNode, err = neighbor.FindNeighbor(info.Node, info.Target)
	return err
}
