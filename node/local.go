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
	data        map[string][]byte
	datalock    *sync.RWMutex
	peers       *Peers
	dims        uint
	peerlock    *sync.RWMutex
	operational *sync.RWMutex
	mutex       *common.IntExtLock
	sem         *util.Semaphore
}

func CheckItems(local *Local, key string) bool {
	_, found := local.data[key]
	return found
}

func PrintPeers(local *Local) {
	fmt.Print(local.Node.ID, ": ")
	for i, peer := range local.peers.peers {
		if peer != nil {
			fmt.Print("{", peer.ID(), ",", i, ",", local.Node.ID^(1<<uint(i))^peer.ID(), "} ")
		}
	}
	fmt.Println()
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
		dims:        dims,
		data:        make(map[string][]byte),
		datalock:    &sync.RWMutex{},
		peers:       nil,
		peerlock:    &sync.RWMutex{},
		operational: &sync.RWMutex{},
		mutex:       common.NewIntExtLock(),
		sem:         util.NewSemaphore(maxrequests),
	}
	local.startRPC()
	if seedaddr != "" {
		local.operational.Lock()
		defer local.operational.Unlock()
		peer, err := NewPeer(seedaddr)

		//Get the number of dimensions
		dims, err = peer.GetDims()
		if err != nil {
			return nil, err
		}
		id, err = peer.AssistBootstrap(fmt.Sprintf("%v:%v", ip, port))
		local.peers = NewPeers(id, dims)
		if err != nil {
			fmt.Println(err)
			return nil, err
		}
		local.Node.ID = id
		local.dims = dims
		//fmt.Println("release", id)
		local.getPeers(peer)
		if err != nil {
			return nil, err
		}

	} else {
		local.peers = NewPeers(id, dims)
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
func (local *Local) updateStore() {
	local.mutex.RIntLock()
	local.datalock.Lock()
	for key := range local.data {
		keyid := keyToID(key, local.dims)
		peer := local.peers.GetClosest(keyid)[0]
		if peer == nil {
			continue
		}
		err := peer.Relocate(key, local.data[key])
		if err != nil {
			fmt.Println(err)
			continue
		}
		delete(local.data, key)
		//fmt.Println("transfered", key, "to", peer.ID(), keyToID(key, len(local.peers)))
	}
	local.datalock.Unlock()
	local.mutex.RIntUnlock()

}

//getPeers initially populates the node with a set of ideally optimal peers
//Local Method
func (local *Local) getPeers(peer RPCNodeProxy) {
	//time.Sleep(2 * time.Second)
	if peer == nil {
		return
	}
	local.mutex.RIntLock()
	defer local.mutex.RIntUnlock()
	blacklist := make(map[uint64]struct{})
	for i := uint(0); i < local.dims; i++ {
		newPeerNode, err := peer.FindPeer(&local.Node, local.Node.ID^(1<<i))
		if err != nil {
			fmt.Println(err)
			continue
		}
		blacklist[newPeerNode.ID] = struct{}{}
		//fmt.Println("Suggested:  ", newPeerNode.ID)
		//fmt.Println(local.Node.ID, "found", newPeerNode.ID, "while looking for", local.Node.ID^(1<<uint(i)), "for slot", i, newPeerNode.ID^local.Node.ID^(1<<uint(i)))
		local.peers.Add(&Peer{Node: newPeerNode})
	}
}

//GetDims returns the number of dimensions in the dht
//RPC Method
func (local *Local) GetDims(_ *struct{}, dims *uint) error {
	local.operational.RLock()
	defer local.operational.RUnlock()
	*dims = local.dims
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
	keyid := keyToID(*key, local.dims)
	closest := local.peers.GetClosest(keyid)
	local.datalock.RLock()
	val, found := local.data[*key]
	local.datalock.RUnlock()
	if found && closest[0] != nil && local.Node.ID^keyid > closest[0].ID()^keyid {
		go local.updateStore()
	}
	if found {
		*data = val
		return nil
	}
	if closest[0] == nil || local.Node.ID^keyid < closest[0].ID()^keyid {
		return NewKeyNotFound(*key, local.Node.ID)
	}
	var err error
	local.sem.Lock()
	defer local.sem.Unlock()
	for _, peer := range closest {
		*data, err = peer.Get(*key)
		if err == nil {
			return nil
		}
	}
	return NewKeyNotFound(*key, local.Node.ID)
}

//Relocate moves data/key to the optimal location
//
func (local *Local) Relocate(item *common.Item, _ *struct{}) error {

	local.mutex.RIntLock()
	defer local.mutex.RIntUnlock()
	local.sem.Lock()
	defer local.sem.Unlock()
	closest := local.peers.GetClosest(keyToID(item.Key, local.dims))

	if closest == nil {
		//fmt.Println(item.Data)
		local.datalock.Lock()
		local.data[item.Key] = item.Data
		local.datalock.Unlock()
		return nil
	}
	return closest[0].Relocate(item.Key, item.Data)
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
	closest := local.peers.GetClosest(keyToID(item.Key, local.dims))
	fmt.Println("ghg", keyToID(item.Key, local.dims))
	for _, close := range closest {
		if close != nil {
			fmt.Print("  ", close.ID())
		} else {
			fmt.Print("  ", nil)
		}
	}
	fmt.Println()

	if closest[0] == nil {
		//fmt.Println(item.Data)
		local.datalock.Lock()
		local.data[item.Key] = item.Data
		local.datalock.Unlock()
		return nil
	}

	return closest[0].Set(item.Key, item.Data)
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
	keyid := keyToID(*key, local.dims)
	closest := local.peers.GetClosest(keyid)

	if closest[0] == nil {
		local.datalock.Lock()
		delete(local.data, *key)
		local.datalock.Unlock()
		return nil
	}

	//Calls RPC and returns err (if any)
	return closest[0].Del(*key)
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

//GetPeers returns the peers of the local node
//RPC Method
func (local *Local) GetPeers(_ *struct{}, peers *[]RPCNodeProxy) error {
	local.operational.RLock()
	defer local.operational.RUnlock()
	local.mutex.RExtLock()
	//fmt.Println("GetPeers")
	local.sem.Lock()
	defer local.sem.Unlock()
	defer local.mutex.RExtUnlock()
	*peers = local.peers.GetPeers()
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

	next := nextPeer(local.Node.ID) - 1

	local.peerlock.Lock()
	for i, peer := range local.peers.GetPeers() {
		optimalID := local.Node.ID ^ (1 << uint(i))
		//If the slot for a peer differing by the {next} bit is empty or occupied by a sub-optimal peer
		if peer == nil || peer.ID() != optimalID {
			//Assign the optimal id so that it is returned to the sender
			*id = optimalID

			//Initialize the new peer
			newpeer, err := makePeer(*addr, optimalID)
			//Make sure everything worked, otherwise convey failure
			if err != nil {
				local.peerlock.Unlock()
				return err
			}

			//Save old contents, place new peer, and update peers
			local.peers.Place(newpeer, i)
			local.peerlock.Unlock()
			//fmt.Println(newPeer.ID(), "checked", local.Node.Port)
			return nil
		}
	}
	local.peerlock.Unlock()
	var err error
	//local.sem.Lock()
	fmt.Println(next, local.dims)
	*id, err = local.peers.Get(next).AssistBootstrap(*addr)
	//local.sem.Unlock()
	return err
}

//FindPeer attempts to find the most optimal neighhbor available
//RPCMethod
func (local *Local) FindPeer(info *FindMsg, peerNode *Node) (err error) {
	local.operational.RLock()
	defer local.operational.RUnlock()
	local.sem.Lock()
	defer local.sem.Unlock()

	var closest RPCNodeProxy
	closestID := local.Node.ID
	for _, peer := range local.peers.GetPeers() {
		if peer == nil || peer.ID() == info.ID {
			continue
		}
		//fmt.Println(closestID, peer.ID(), info.Target, closestID^info.Target, peer.ID()^info.Target, i)
		fmt.Println(local.ID, closestID, peer.ID(), info.Target, info.ID)
		if isCloser(closestID, peer.ID(), info.Target) {
			closest = peer
			closestID = peer.ID()
		}
	}

	if closest == nil || local.Node.ID == info.Target {
		local.mutex.RIntLock()
		defer local.mutex.RIntUnlock()
		local.peers.Add(&Peer{Node: *info.Node})
		*peerNode = local.Node
		return err
	}
	*peerNode, err = closest.FindPeer(info.Node, info.Target)
	return err
}
