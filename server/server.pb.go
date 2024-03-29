// Code generated by protoc-gen-go. DO NOT EDIT.
// source: server.proto

package server

import (
	context "context"
	fmt "fmt"
	proto "github.com/golang/protobuf/proto"
	grpc "google.golang.org/grpc"
	math "math"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion3 // please upgrade the proto package

type GetRequest struct {
	Key                  string   `protobuf:"bytes,1,opt,name=key,proto3" json:"key,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *GetRequest) Reset()         { *m = GetRequest{} }
func (m *GetRequest) String() string { return proto.CompactTextString(m) }
func (*GetRequest) ProtoMessage()    {}
func (*GetRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_ad098daeda4239f7, []int{0}
}

func (m *GetRequest) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_GetRequest.Unmarshal(m, b)
}
func (m *GetRequest) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_GetRequest.Marshal(b, m, deterministic)
}
func (m *GetRequest) XXX_Merge(src proto.Message) {
	xxx_messageInfo_GetRequest.Merge(m, src)
}
func (m *GetRequest) XXX_Size() int {
	return xxx_messageInfo_GetRequest.Size(m)
}
func (m *GetRequest) XXX_DiscardUnknown() {
	xxx_messageInfo_GetRequest.DiscardUnknown(m)
}

var xxx_messageInfo_GetRequest proto.InternalMessageInfo

func (m *GetRequest) GetKey() string {
	if m != nil {
		return m.Key
	}
	return ""
}

type GetResponse struct {
	Status               string   `protobuf:"bytes,1,opt,name=status,proto3" json:"status,omitempty"`
	Value                string   `protobuf:"bytes,2,opt,name=value,proto3" json:"value,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *GetResponse) Reset()         { *m = GetResponse{} }
func (m *GetResponse) String() string { return proto.CompactTextString(m) }
func (*GetResponse) ProtoMessage()    {}
func (*GetResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_ad098daeda4239f7, []int{1}
}

func (m *GetResponse) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_GetResponse.Unmarshal(m, b)
}
func (m *GetResponse) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_GetResponse.Marshal(b, m, deterministic)
}
func (m *GetResponse) XXX_Merge(src proto.Message) {
	xxx_messageInfo_GetResponse.Merge(m, src)
}
func (m *GetResponse) XXX_Size() int {
	return xxx_messageInfo_GetResponse.Size(m)
}
func (m *GetResponse) XXX_DiscardUnknown() {
	xxx_messageInfo_GetResponse.DiscardUnknown(m)
}

var xxx_messageInfo_GetResponse proto.InternalMessageInfo

func (m *GetResponse) GetStatus() string {
	if m != nil {
		return m.Status
	}
	return ""
}

func (m *GetResponse) GetValue() string {
	if m != nil {
		return m.Value
	}
	return ""
}

type SetRequest struct {
	Key                  string   `protobuf:"bytes,1,opt,name=key,proto3" json:"key,omitempty"`
	Value                string   `protobuf:"bytes,2,opt,name=value,proto3" json:"value,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *SetRequest) Reset()         { *m = SetRequest{} }
func (m *SetRequest) String() string { return proto.CompactTextString(m) }
func (*SetRequest) ProtoMessage()    {}
func (*SetRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_ad098daeda4239f7, []int{2}
}

func (m *SetRequest) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_SetRequest.Unmarshal(m, b)
}
func (m *SetRequest) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_SetRequest.Marshal(b, m, deterministic)
}
func (m *SetRequest) XXX_Merge(src proto.Message) {
	xxx_messageInfo_SetRequest.Merge(m, src)
}
func (m *SetRequest) XXX_Size() int {
	return xxx_messageInfo_SetRequest.Size(m)
}
func (m *SetRequest) XXX_DiscardUnknown() {
	xxx_messageInfo_SetRequest.DiscardUnknown(m)
}

var xxx_messageInfo_SetRequest proto.InternalMessageInfo

func (m *SetRequest) GetKey() string {
	if m != nil {
		return m.Key
	}
	return ""
}

func (m *SetRequest) GetValue() string {
	if m != nil {
		return m.Value
	}
	return ""
}

type SetResponse struct {
	Status               string   `protobuf:"bytes,1,opt,name=status,proto3" json:"status,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *SetResponse) Reset()         { *m = SetResponse{} }
func (m *SetResponse) String() string { return proto.CompactTextString(m) }
func (*SetResponse) ProtoMessage()    {}
func (*SetResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_ad098daeda4239f7, []int{3}
}

func (m *SetResponse) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_SetResponse.Unmarshal(m, b)
}
func (m *SetResponse) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_SetResponse.Marshal(b, m, deterministic)
}
func (m *SetResponse) XXX_Merge(src proto.Message) {
	xxx_messageInfo_SetResponse.Merge(m, src)
}
func (m *SetResponse) XXX_Size() int {
	return xxx_messageInfo_SetResponse.Size(m)
}
func (m *SetResponse) XXX_DiscardUnknown() {
	xxx_messageInfo_SetResponse.DiscardUnknown(m)
}

var xxx_messageInfo_SetResponse proto.InternalMessageInfo

func (m *SetResponse) GetStatus() string {
	if m != nil {
		return m.Status
	}
	return ""
}

type DeleteRequest struct {
	Key                  string   `protobuf:"bytes,1,opt,name=key,proto3" json:"key,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *DeleteRequest) Reset()         { *m = DeleteRequest{} }
func (m *DeleteRequest) String() string { return proto.CompactTextString(m) }
func (*DeleteRequest) ProtoMessage()    {}
func (*DeleteRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_ad098daeda4239f7, []int{4}
}

func (m *DeleteRequest) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_DeleteRequest.Unmarshal(m, b)
}
func (m *DeleteRequest) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_DeleteRequest.Marshal(b, m, deterministic)
}
func (m *DeleteRequest) XXX_Merge(src proto.Message) {
	xxx_messageInfo_DeleteRequest.Merge(m, src)
}
func (m *DeleteRequest) XXX_Size() int {
	return xxx_messageInfo_DeleteRequest.Size(m)
}
func (m *DeleteRequest) XXX_DiscardUnknown() {
	xxx_messageInfo_DeleteRequest.DiscardUnknown(m)
}

var xxx_messageInfo_DeleteRequest proto.InternalMessageInfo

func (m *DeleteRequest) GetKey() string {
	if m != nil {
		return m.Key
	}
	return ""
}

type DeleteResponse struct {
	Status               string   `protobuf:"bytes,1,opt,name=status,proto3" json:"status,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *DeleteResponse) Reset()         { *m = DeleteResponse{} }
func (m *DeleteResponse) String() string { return proto.CompactTextString(m) }
func (*DeleteResponse) ProtoMessage()    {}
func (*DeleteResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_ad098daeda4239f7, []int{5}
}

func (m *DeleteResponse) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_DeleteResponse.Unmarshal(m, b)
}
func (m *DeleteResponse) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_DeleteResponse.Marshal(b, m, deterministic)
}
func (m *DeleteResponse) XXX_Merge(src proto.Message) {
	xxx_messageInfo_DeleteResponse.Merge(m, src)
}
func (m *DeleteResponse) XXX_Size() int {
	return xxx_messageInfo_DeleteResponse.Size(m)
}
func (m *DeleteResponse) XXX_DiscardUnknown() {
	xxx_messageInfo_DeleteResponse.DiscardUnknown(m)
}

var xxx_messageInfo_DeleteResponse proto.InternalMessageInfo

func (m *DeleteResponse) GetStatus() string {
	if m != nil {
		return m.Status
	}
	return ""
}

type SyncRequest struct {
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *SyncRequest) Reset()         { *m = SyncRequest{} }
func (m *SyncRequest) String() string { return proto.CompactTextString(m) }
func (*SyncRequest) ProtoMessage()    {}
func (*SyncRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_ad098daeda4239f7, []int{6}
}

func (m *SyncRequest) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_SyncRequest.Unmarshal(m, b)
}
func (m *SyncRequest) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_SyncRequest.Marshal(b, m, deterministic)
}
func (m *SyncRequest) XXX_Merge(src proto.Message) {
	xxx_messageInfo_SyncRequest.Merge(m, src)
}
func (m *SyncRequest) XXX_Size() int {
	return xxx_messageInfo_SyncRequest.Size(m)
}
func (m *SyncRequest) XXX_DiscardUnknown() {
	xxx_messageInfo_SyncRequest.DiscardUnknown(m)
}

var xxx_messageInfo_SyncRequest proto.InternalMessageInfo

type SyncResponse struct {
	Status               string   `protobuf:"bytes,1,opt,name=status,proto3" json:"status,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *SyncResponse) Reset()         { *m = SyncResponse{} }
func (m *SyncResponse) String() string { return proto.CompactTextString(m) }
func (*SyncResponse) ProtoMessage()    {}
func (*SyncResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_ad098daeda4239f7, []int{7}
}

func (m *SyncResponse) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_SyncResponse.Unmarshal(m, b)
}
func (m *SyncResponse) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_SyncResponse.Marshal(b, m, deterministic)
}
func (m *SyncResponse) XXX_Merge(src proto.Message) {
	xxx_messageInfo_SyncResponse.Merge(m, src)
}
func (m *SyncResponse) XXX_Size() int {
	return xxx_messageInfo_SyncResponse.Size(m)
}
func (m *SyncResponse) XXX_DiscardUnknown() {
	xxx_messageInfo_SyncResponse.DiscardUnknown(m)
}

var xxx_messageInfo_SyncResponse proto.InternalMessageInfo

func (m *SyncResponse) GetStatus() string {
	if m != nil {
		return m.Status
	}
	return ""
}

func init() {
	proto.RegisterType((*GetRequest)(nil), "server.GetRequest")
	proto.RegisterType((*GetResponse)(nil), "server.GetResponse")
	proto.RegisterType((*SetRequest)(nil), "server.SetRequest")
	proto.RegisterType((*SetResponse)(nil), "server.SetResponse")
	proto.RegisterType((*DeleteRequest)(nil), "server.DeleteRequest")
	proto.RegisterType((*DeleteResponse)(nil), "server.DeleteResponse")
	proto.RegisterType((*SyncRequest)(nil), "server.SyncRequest")
	proto.RegisterType((*SyncResponse)(nil), "server.SyncResponse")
}

func init() { proto.RegisterFile("server.proto", fileDescriptor_ad098daeda4239f7) }

var fileDescriptor_ad098daeda4239f7 = []byte{
	// 248 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0xe2, 0xe2, 0x29, 0x4e, 0x2d, 0x2a,
	0x4b, 0x2d, 0xd2, 0x2b, 0x28, 0xca, 0x2f, 0xc9, 0x17, 0x62, 0x83, 0xf0, 0x94, 0xe4, 0xb8, 0xb8,
	0xdc, 0x53, 0x4b, 0x82, 0x52, 0x0b, 0x4b, 0x53, 0x8b, 0x4b, 0x84, 0x04, 0xb8, 0x98, 0xb3, 0x53,
	0x2b, 0x25, 0x18, 0x15, 0x18, 0x35, 0x38, 0x83, 0x40, 0x4c, 0x25, 0x6b, 0x2e, 0x6e, 0xb0, 0x7c,
	0x71, 0x41, 0x7e, 0x5e, 0x71, 0xaa, 0x90, 0x18, 0x17, 0x5b, 0x71, 0x49, 0x62, 0x49, 0x69, 0x31,
	0x54, 0x0d, 0x94, 0x27, 0x24, 0xc2, 0xc5, 0x5a, 0x96, 0x98, 0x53, 0x9a, 0x2a, 0xc1, 0x04, 0x16,
	0x86, 0x70, 0x94, 0x4c, 0xb8, 0xb8, 0x82, 0xf1, 0x18, 0x8e, 0x43, 0x97, 0x2a, 0x17, 0x77, 0x30,
	0x61, 0x2b, 0x95, 0x14, 0xb9, 0x78, 0x5d, 0x52, 0x73, 0x52, 0x4b, 0x52, 0x71, 0x3b, 0x5e, 0x83,
	0x8b, 0x0f, 0xa6, 0x84, 0x80, 0x61, 0xbc, 0x5c, 0xdc, 0xc1, 0x95, 0x79, 0xc9, 0x50, 0xa3, 0x94,
	0xd4, 0xb8, 0x78, 0x20, 0x5c, 0xfc, 0xda, 0x8c, 0x6e, 0x31, 0x72, 0xb1, 0x38, 0xfa, 0xb9, 0x38,
	0x09, 0x19, 0x70, 0x31, 0xbb, 0xa7, 0x96, 0x08, 0x09, 0xe9, 0x41, 0x03, 0x19, 0x11, 0xa6, 0x52,
	0xc2, 0x28, 0x62, 0x10, 0x03, 0x95, 0x18, 0x40, 0x3a, 0x82, 0x91, 0x75, 0x04, 0x63, 0xd1, 0x11,
	0x8c, 0xa2, 0xc3, 0x92, 0x8b, 0x0d, 0xe2, 0x1b, 0x21, 0x51, 0x98, 0x02, 0x94, 0x00, 0x90, 0x12,
	0x43, 0x17, 0x86, 0x6b, 0x35, 0xe6, 0x62, 0x01, 0xf9, 0x47, 0x08, 0x61, 0x32, 0xc2, 0xb3, 0x52,
	0x22, 0xa8, 0x82, 0x30, 0x4d, 0x49, 0x6c, 0xe0, 0x94, 0x62, 0x0c, 0x08, 0x00, 0x00, 0xff, 0xff,
	0xff, 0x54, 0x58, 0x8b, 0x39, 0x02, 0x00, 0x00,
}

// Reference imports to suppress errors if they are not otherwise used.
var _ context.Context
var _ grpc.ClientConn

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
const _ = grpc.SupportPackageIsVersion4

// ANDBClient is the client API for ANDB service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://godoc.org/google.golang.org/grpc#ClientConn.NewStream.
type ANDBClient interface {
	Get(ctx context.Context, in *GetRequest, opts ...grpc.CallOption) (*GetResponse, error)
	Set(ctx context.Context, in *SetRequest, opts ...grpc.CallOption) (*SetResponse, error)
	Delete(ctx context.Context, in *DeleteRequest, opts ...grpc.CallOption) (*DeleteResponse, error)
	Sync(ctx context.Context, in *SyncRequest, opts ...grpc.CallOption) (*SyncResponse, error)
}

type aNDBClient struct {
	cc *grpc.ClientConn
}

func NewANDBClient(cc *grpc.ClientConn) ANDBClient {
	return &aNDBClient{cc}
}

func (c *aNDBClient) Get(ctx context.Context, in *GetRequest, opts ...grpc.CallOption) (*GetResponse, error) {
	out := new(GetResponse)
	err := c.cc.Invoke(ctx, "/server.ANDB/Get", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *aNDBClient) Set(ctx context.Context, in *SetRequest, opts ...grpc.CallOption) (*SetResponse, error) {
	out := new(SetResponse)
	err := c.cc.Invoke(ctx, "/server.ANDB/Set", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *aNDBClient) Delete(ctx context.Context, in *DeleteRequest, opts ...grpc.CallOption) (*DeleteResponse, error) {
	out := new(DeleteResponse)
	err := c.cc.Invoke(ctx, "/server.ANDB/Delete", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *aNDBClient) Sync(ctx context.Context, in *SyncRequest, opts ...grpc.CallOption) (*SyncResponse, error) {
	out := new(SyncResponse)
	err := c.cc.Invoke(ctx, "/server.ANDB/Sync", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ANDBServer is the server API for ANDB service.
type ANDBServer interface {
	Get(context.Context, *GetRequest) (*GetResponse, error)
	Set(context.Context, *SetRequest) (*SetResponse, error)
	Delete(context.Context, *DeleteRequest) (*DeleteResponse, error)
	Sync(context.Context, *SyncRequest) (*SyncResponse, error)
}

func RegisterANDBServer(s *grpc.Server, srv ANDBServer) {
	s.RegisterService(&_ANDB_serviceDesc, srv)
}

func _ANDB_Get_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ANDBServer).Get(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/server.ANDB/Get",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ANDBServer).Get(ctx, req.(*GetRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ANDB_Set_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(SetRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ANDBServer).Set(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/server.ANDB/Set",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ANDBServer).Set(ctx, req.(*SetRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ANDB_Delete_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(DeleteRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ANDBServer).Delete(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/server.ANDB/Delete",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ANDBServer).Delete(ctx, req.(*DeleteRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ANDB_Sync_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(SyncRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ANDBServer).Sync(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/server.ANDB/Sync",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ANDBServer).Sync(ctx, req.(*SyncRequest))
	}
	return interceptor(ctx, in, info, handler)
}

var _ANDB_serviceDesc = grpc.ServiceDesc{
	ServiceName: "server.ANDB",
	HandlerType: (*ANDBServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Get",
			Handler:    _ANDB_Get_Handler,
		},
		{
			MethodName: "Set",
			Handler:    _ANDB_Set_Handler,
		},
		{
			MethodName: "Delete",
			Handler:    _ANDB_Delete_Handler,
		},
		{
			MethodName: "Sync",
			Handler:    _ANDB_Sync_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "server.proto",
}
