package node

import "github.com/rhino1998/hypercube/common"

//RPCNode is a general interface representing a node which serves requests over rpc
type RPCNode interface {
	Get(*string, *[]byte) error
	Set(*common.Item, *struct{}) error
	Relocate(*common.Item, *struct{}) error
	Del(*string, *struct{}) error
	AssistBootstrap(*string, *uint64) error
	FindNeighbor(*FindMsg, *Node) error
	GetNeighbors(*struct{}, *[]RPCNodeProxy) error
	Pong(*struct{}, *struct{}) error
	Info(*struct{}, *Node) error
}

//RPCNodeProxy is a general interface representing a proxy for an RPCNode
type RPCNodeProxy interface {
	Get(string) ([]byte, error)
	Set(string, []byte) error
	Relocate(string, []byte) error
	Del(string) error
	Ping() error
	ID() uint64
	IP() string
	Port() int
	Info() Node
	FindNeighbor(*Node, uint64) (Node, error)
	AssistBootstrap(string) (uint64, error)
	GetNeighbors() ([]RPCNodeProxy, error)
	Connect() error
	Close() error
}

type Node struct {
	ID   uint64 `json:"id"`
	IP   string `json:"addr"`
	Port int    `json:"port"`
}

type FindMsg struct {
	Target uint64 `json:"target"`
	*Node
}
