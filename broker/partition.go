package broker

type partition struct {
	name string
	addr string
	group map[string] *group
}

func newPartition(name, addr string)  *partition{
	return &partition{
		name : name,
		addr : addr,
		group: make(map[string]*group),
	}
}