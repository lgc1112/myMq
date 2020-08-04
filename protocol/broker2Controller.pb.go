// Code generated by protoc-gen-go.
// source: broker2Controller.proto
// DO NOT EDIT!

package protocol

import proto "github.com/golang/protobuf/proto"
import fmt "fmt"
import math "math"

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

type Broker2ControllerKey int32

const (
	Broker2ControllerKey_Heartbeat              Broker2ControllerKey = 0
	Broker2ControllerKey_RegisterBroker         Broker2ControllerKey = 1
	Broker2ControllerKey_unRegisterBroker       Broker2ControllerKey = 2
	Broker2ControllerKey_CreadtPartitionSuccess Broker2ControllerKey = 3
	Broker2ControllerKey_CreadtPartitionError   Broker2ControllerKey = 4
)

var Broker2ControllerKey_name = map[int32]string{
	0: "Heartbeat",
	1: "RegisterBroker",
	2: "unRegisterBroker",
	3: "CreadtPartitionSuccess",
	4: "CreadtPartitionError",
}
var Broker2ControllerKey_value = map[string]int32{
	"Heartbeat":              0,
	"RegisterBroker":         1,
	"unRegisterBroker":       2,
	"CreadtPartitionSuccess": 3,
	"CreadtPartitionError":   4,
}

func (x Broker2ControllerKey) String() string {
	return proto.EnumName(Broker2ControllerKey_name, int32(x))
}
func (Broker2ControllerKey) EnumDescriptor() ([]byte, []int) { return fileDescriptor1, []int{0} }

type Broker2Controller struct {
	Key  Broker2ControllerKey `protobuf:"varint,1,opt,name=key,enum=protocol.Broker2ControllerKey" json:"key,omitempty"`
	Addr *ListenAddr          `protobuf:"bytes,2,opt,name=addr" json:"addr,omitempty"`
}

func (m *Broker2Controller) Reset()                    { *m = Broker2Controller{} }
func (m *Broker2Controller) String() string            { return proto.CompactTextString(m) }
func (*Broker2Controller) ProtoMessage()               {}
func (*Broker2Controller) Descriptor() ([]byte, []int) { return fileDescriptor1, []int{0} }

func (m *Broker2Controller) GetKey() Broker2ControllerKey {
	if m != nil {
		return m.Key
	}
	return Broker2ControllerKey_Heartbeat
}

func (m *Broker2Controller) GetAddr() *ListenAddr {
	if m != nil {
		return m.Addr
	}
	return nil
}

func init() {
	proto.RegisterType((*Broker2Controller)(nil), "protocol.Broker2Controller")
	proto.RegisterEnum("protocol.Broker2ControllerKey", Broker2ControllerKey_name, Broker2ControllerKey_value)
}

func init() { proto.RegisterFile("broker2Controller.proto", fileDescriptor1) }

var fileDescriptor1 = []byte{
	// 219 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x6c, 0x8e, 0xc1, 0x4a, 0xc3, 0x40,
	0x10, 0x40, 0xdd, 0xb6, 0x88, 0x8e, 0xb4, 0xac, 0xc3, 0xa2, 0xa1, 0x07, 0x29, 0x9e, 0x82, 0x87,
	0x20, 0xf1, 0x0b, 0xb4, 0x08, 0x82, 0x1e, 0x24, 0x7e, 0xc1, 0x26, 0x3b, 0x94, 0xd0, 0xb8, 0x23,
	0xb3, 0xd3, 0x43, 0x3f, 0xc0, 0xff, 0x96, 0x26, 0x88, 0x10, 0x7b, 0x1a, 0x98, 0xf7, 0xde, 0x30,
	0x70, 0x5d, 0x0b, 0x6f, 0x49, 0xca, 0x35, 0x47, 0x15, 0xee, 0x3a, 0x92, 0xe2, 0x4b, 0x58, 0x19,
	0xcf, 0xfa, 0xd1, 0x70, 0xb7, 0x9c, 0x7f, 0x52, 0x4a, 0x7e, 0x43, 0x03, 0xb8, 0x65, 0xb8, 0x7c,
	0x1a, 0x37, 0x78, 0x0f, 0xd3, 0x2d, 0xed, 0x33, 0xb3, 0x32, 0xf9, 0xa2, 0xbc, 0x29, 0x7e, 0xdb,
	0xe2, 0x9f, 0xf9, 0x4a, 0xfb, 0xea, 0xa0, 0x62, 0x0e, 0x33, 0x1f, 0x82, 0x64, 0x93, 0x95, 0xc9,
	0x2f, 0x4a, 0xf7, 0x97, 0xbc, 0xb5, 0x49, 0x29, 0x3e, 0x86, 0x20, 0x55, 0x6f, 0xdc, 0x7d, 0x1b,
	0x70, 0xc7, 0xee, 0xe0, 0x1c, 0xce, 0x5f, 0xc8, 0x8b, 0xd6, 0xe4, 0xd5, 0x9e, 0x20, 0xc2, 0xa2,
	0xa2, 0xcd, 0xa1, 0x96, 0x41, 0xb7, 0x06, 0x1d, 0xd8, 0x5d, 0x1c, 0x6d, 0x27, 0xb8, 0x84, 0xab,
	0xb5, 0x90, 0x0f, 0xfa, 0xee, 0x45, 0x5b, 0x6d, 0x39, 0x7e, 0xec, 0x9a, 0x86, 0x52, 0xb2, 0x53,
	0xcc, 0xc0, 0x8d, 0xd8, 0xb3, 0x08, 0x8b, 0x9d, 0xd5, 0xa7, 0xfd, 0x8b, 0x0f, 0x3f, 0x01, 0x00,
	0x00, 0xff, 0xff, 0x2f, 0x0e, 0x24, 0xab, 0x33, 0x01, 0x00, 0x00,
}
