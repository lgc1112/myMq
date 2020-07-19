package broker
import (
	"../protocol"
)
type partionGroup struct {
	msgChan chan *protocol.Message
}

func newPartionGroup() *partionGroup  {
	g := &partionGroup{
		msgChan: nil,
	}
	return g
}

func (g *partionGroup)putMsg(msg *protocol.Message)  {
	g.msgChan <- msg
}