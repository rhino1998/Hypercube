package node

import (
	"errors"
	"fmt"
)

//ErrorNilNeighbor is an error returned when a neighbor is Nil
var ErrorNilNeighbor = errors.New("Nil neighbor")

//KeyNotFound describes the error with a missing key
type KeyNotFound struct {
	key string
	id  uint64
}

//NewKeyNotFound makes a new custom KeyNotFound error
func NewKeyNotFound(key string, id uint64) *KeyNotFound {
	return &KeyNotFound{
		key: key,
		id:  id,
	}
}

func (err *KeyNotFound) Error() string {
	return fmt.Sprintf("Key %s not found in node ID %v", err.key, err.id)
}
