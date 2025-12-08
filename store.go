package main
import(
	"sync"
)

var store = make(map[string]string)
var mu sync.RWMutex