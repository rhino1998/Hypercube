package node

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
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
	operational  *sync.RWMutex
	mutex        *common.IntExtLock
	sem          *util.Semaphore
}

//NewLocal makes new local node at a specified port in a dht of dim dimensions. Dim is overridden by the seednode if seenode is defined
func NewLocal(port int, dims uint, maxrequests int, seedaddr string) (*Local, error) {
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
		operational:  &sync.RWMutex{},
		mutex:        common.NewIntExtLock(),
		sem:          util.NewSemaphore(maxrequests),
	}
	local.startRPC()
	if seedaddr != "" {
		local.operational.Lock()
		defer local.operational.Unlock()
		neighbor, err := NewNeighbor(seedaddr)

		//Get the number of dimensions
		dims, err = neighbor.GetDims()
		if err != nil {
			return nil, err
		}
		local.neighbors = make([]RPCNodeProxy, dims)
		id, err = neighbor.AssistBootstrap(fmt.Sprintf("%v:%v", ip, port))
		if err != nil {
			fmt.Println(err)
			return nil, err
		}
		local.Node.ID = id
		//fmt.Println("release", id)
		local.mutex.RIntLock()
		local.updateNeighbors(neighbor)
		local.mutex.RIntUnlock()
		local.getNeighbors(neighbor)
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
	for key := range local.data {
		local.datalock.Lock()
		if keyToID(key, len(local.neighbors)) == 110 {
			fmt.Println(local.ID, "moving", key, "to", neighbor.ID())
		}
		if !isCloser(local.Node.ID, neighbor.ID(), keyToID(key, len(local.neighbors))) {
			local.datalock.Unlock()
			continue
		}
		err := neighbor.Relocate(key, local.data[key])
		if err != nil {
			local.datalock.Unlock()
			continue
		}
		delete(local.data, key)
		fmt.Println("transfered", key, "to", neighbor.ID())
		local.datalock.Unlock()
	}
	local.mutex.RIntUnlock()

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
		//fmt.Println(local.Node.ID, "found", newNeighborNode.ID, "while looking for", local.Node.ID^(1<<uint(i)), "for slot", i, newNeighborNode.ID^local.Node.ID^(1<<uint(i)))
		newNeighbor, err := NewNeighbor(fmt.Sprintf("%v:%v", newNeighborNode.IP, newNeighborNode.Port))
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
	local.neighborlock.Lock()
	//fmt.Println(newNeighbor.ID())
	for i, neighbor := range local.neighbors {
		if neighbor == nil {
			err := newNeighbor.Connect()
			if err != nil {
				local.neighborlock.Unlock()
				return err
			}

			local.neighbors[i] = newNeighbor
			local.neighborlock.Unlock()
			local.updateStore(newNeighbor)
			return nil
		}
		if neighbor.ID() == newNeighbor.ID() {
			break
		}
		//fmt.Println(neighbor.ID(), newNeighbor.ID(), local.Node.ID^(1<<uint(i)))
		if isCloser(neighbor.ID(), newNeighbor.ID(), local.Node.ID^(1<<uint(i))) {
			//fmt.Println(newNeighbor.ID(), "replacing", neighbor.ID(), "on", local.Node.ID)
			err := newNeighbor.Connect()
			if err != nil {
				local.neighborlock.Unlock()
				return err
			}

			local.neighbors[i] = newNeighbor
			local.neighborlock.Unlock()
			local.updateStore(newNeighbor)
			local.updateNeighbors(neighbor)
			return nil
		}
	}
	local.neighborlock.Unlock()
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
			//fmt.Println("nilf")
			continue
		}
		//fmt.Println(closestID, neighbor.ID(), id, closestID^id, neighbor.ID()^id, i)
		if isCloser(closestID, neighbor.ID(), id) {
			closest = neighbor
			closestID = neighbor.ID()
		}
	}
	if closest == nil || closest.ID() == local.Node.ID {
		//fmt.Println("cant find")
		return nil, nil
	}
	fmt.Println(closest.ID())
	return closest, nil
}

//closestToKey returns the neighbor closest to the key given
//Local Method
func (local *Local) closestToKey(key string) (RPCNodeProxy, error) {
	return local.findID(keyToID(key, len(local.neighbors)))
}

//GetDims returns the number of dimensions in the dht
//RPC Method
func (local *Local) GetDims(_ *struct{}, dims *uint) error {
	local.operational.RLock()
	defer local.operational.RUnlock()
	*dims = uint(len(local.neighbors))
	return nil
}

//Get value from key
//RPC Method
func (local *Local) Get(key *string, data *[]byte) error {
	local.operational.RLock()
	defer local.operational.RUnlock()

	local.mutex.RExtLock()
	local.sem.Lock()
	defer local.sem.Unlock()
	defer local.mutex.RExtUnlock()

	closest, err := local.closestToKey(*key)

	if err != nil {
		return err
	}
	local.datalock.RLock()
	val, found := local.data[*key]
	local.datalock.RUnlock()
	if found {
		//fmt.Println(keyToID(*key, len(local.neighbors)), local.ID)
		*data = val
		return nil
	}
	if !found && closest == nil {
		return NewKeyNotFound(*key, local.Node.ID)
	}

	//fmt.Println(*data)
	//fmt.Println(local.Node.ID, keyToID(*key, len(local.neighbors)))

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
	local.sem.Lock()
	defer local.sem.Unlock()
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
	return closest.Relocate(item.Key, item.Data)
}

//Set a value in dht
//RPC Method
func (local *Local) Set(item *common.Item, _ *struct{}) error {
	local.operational.RLock()
	defer local.operational.RUnlock()

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

//Del deletes a key/data from the dht
//RPC Method
func (local *Local) Del(key *string, _ *struct{}) error {
	local.operational.RLock()
	defer local.operational.RUnlock()

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
	local.operational.RLock()
	defer local.operational.RUnlock()
	local.mutex.RExtLock()
	//fmt.Println("GetNeighbors")
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

	//Lock for safety
	local.operational.RLock()
	local.operational.RUnlock()
	local.mutex.RIntLock()
	defer local.mutex.RIntUnlock()

	next := nextNeighbor(local.Node.ID) - 1
	optimalID := uint64(local.Node.ID ^ (1 << (next)))

	local.neighborlock.Lock()
	//If the slot for a neighbor differing by the {next} bit is empty or occupied by a sub-optimal neighbor
	if local.neighbors[next] == nil || local.neighbors[next].ID() != optimalID {
		//Assign the optimal id so that it is returned to the sender
		*id = optimalID

		//Initialize the new neighbor
		newNeighbor, err := makeNeighbor(*addr, optimalID)
		//Make sure everything worked, otherwise convey failure
		if err != nil {
			local.neighborlock.Unlock()
			return err
		}

		//Save old contents, place new neighbor, and update neighbors
		oldNeighbor := local.neighbors[next]
		local.neighbors[next] = newNeighbor

		local.neighborlock.Unlock()
		if oldNeighbor != nil {
			//fmt.Println(newNeighbor.ID(), "check", local.Node.Port, *addr)
			local.updateNeighbors(oldNeighbor)
		}
		//fmt.Println(newNeighbor.ID(), "checked", local.Node.Port)
		return nil
	}
	local.neighborlock.Unlock()
	var err error
	//local.sem.Lock()
	*id, err = local.neighbors[next].AssistBootstrap(*addr)
	//local.sem.Unlock()
	return err
}

//FindNeighbor attempts to find the most optimal neighhbor available
//RPCMethod
func (local *Local) FindNeighbor(info *FindMsg, neighborNode *Node) (err error) {
	local.operational.RLock()
	defer local.operational.RUnlock()
	local.sem.Lock()
	defer local.sem.Unlock()

	var closest RPCNodeProxy
	closestID := local.Node.ID
	for _, neighbor := range local.neighbors {
		if neighbor == nil || neighbor.ID() == info.ID {
			continue
		}
		//fmt.Println(closestID, neighbor.ID(), info.Target, closestID^info.Target, neighbor.ID()^info.Target, i)
		if isCloser(closestID, neighbor.ID(), info.Target) {
			closest = neighbor
			closestID = neighbor.ID()
		}
	}

	if closest == nil {
		local.mutex.RIntLock()
		defer local.mutex.RIntUnlock()
		local.updateNeighbors(&Neighbor{Node: *info.Node})
		*neighborNode = local.Node
		return err
	}
	*neighborNode, err = closest.FindNeighbor(info.Node, info.Target)
	return err
}
