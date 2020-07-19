package broker

type group struct {
	MsgChan chan *message
}
