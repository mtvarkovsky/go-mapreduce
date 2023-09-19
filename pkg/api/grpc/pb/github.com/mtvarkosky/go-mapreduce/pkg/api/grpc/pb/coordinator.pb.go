// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.31.0
// 	protoc        v4.23.4
// source: coordinator.proto

package pb

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

// NewMapTask - message to create new MapTask
type NewMapTask struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// input_file - input file for MapTask
	InputFile string `protobuf:"bytes,1,opt,name=input_file,json=inputFile,proto3" json:"input_file,omitempty"`
}

func (x *NewMapTask) Reset() {
	*x = NewMapTask{}
	if protoimpl.UnsafeEnabled {
		mi := &file_coordinator_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *NewMapTask) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*NewMapTask) ProtoMessage() {}

func (x *NewMapTask) ProtoReflect() protoreflect.Message {
	mi := &file_coordinator_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use NewMapTask.ProtoReflect.Descriptor instead.
func (*NewMapTask) Descriptor() ([]byte, []int) {
	return file_coordinator_proto_rawDescGZIP(), []int{0}
}

func (x *NewMapTask) GetInputFile() string {
	if x != nil {
		return x.InputFile
	}
	return ""
}

// MapTask - message representing MapTask
type MapTask struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// id - id of the MapTask
	Id string `protobuf:"bytes,1,opt,name=id,proto3" json:"id,omitempty"`
	// input_file - input file for MapTask
	InputFile string `protobuf:"bytes,2,opt,name=input_file,json=inputFile,proto3" json:"input_file,omitempty"`
}

func (x *MapTask) Reset() {
	*x = MapTask{}
	if protoimpl.UnsafeEnabled {
		mi := &file_coordinator_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *MapTask) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*MapTask) ProtoMessage() {}

func (x *MapTask) ProtoReflect() protoreflect.Message {
	mi := &file_coordinator_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use MapTask.ProtoReflect.Descriptor instead.
func (*MapTask) Descriptor() ([]byte, []int) {
	return file_coordinator_proto_rawDescGZIP(), []int{1}
}

func (x *MapTask) GetId() string {
	if x != nil {
		return x.Id
	}
	return ""
}

func (x *MapTask) GetInputFile() string {
	if x != nil {
		return x.InputFile
	}
	return ""
}

// MapTaskResult - result of MapTask execution returned by a Worker
type MapTaskResult struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// task_id - id of the MapTask
	TaskId string `protobuf:"bytes,1,opt,name=task_id,json=taskId,proto3" json:"task_id,omitempty"`
	// output_files - intermediate results of the MapTask
	OutputFiles []string `protobuf:"bytes,2,rep,name=output_files,json=outputFiles,proto3" json:"output_files,omitempty"`
	// error - error that have occurred during execution of the MapTask by a Worker
	Error *string `protobuf:"bytes,3,opt,name=error,proto3,oneof" json:"error,omitempty"`
}

func (x *MapTaskResult) Reset() {
	*x = MapTaskResult{}
	if protoimpl.UnsafeEnabled {
		mi := &file_coordinator_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *MapTaskResult) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*MapTaskResult) ProtoMessage() {}

func (x *MapTaskResult) ProtoReflect() protoreflect.Message {
	mi := &file_coordinator_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use MapTaskResult.ProtoReflect.Descriptor instead.
func (*MapTaskResult) Descriptor() ([]byte, []int) {
	return file_coordinator_proto_rawDescGZIP(), []int{2}
}

func (x *MapTaskResult) GetTaskId() string {
	if x != nil {
		return x.TaskId
	}
	return ""
}

func (x *MapTaskResult) GetOutputFiles() []string {
	if x != nil {
		return x.OutputFiles
	}
	return nil
}

func (x *MapTaskResult) GetError() string {
	if x != nil && x.Error != nil {
		return *x.Error
	}
	return ""
}

// NewReduceTask - message to create new ReduceTask
type NewReduceTask struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// input_files - input files for ReduceTask
	InputFiles []string `protobuf:"bytes,1,rep,name=input_files,json=inputFiles,proto3" json:"input_files,omitempty"`
}

func (x *NewReduceTask) Reset() {
	*x = NewReduceTask{}
	if protoimpl.UnsafeEnabled {
		mi := &file_coordinator_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *NewReduceTask) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*NewReduceTask) ProtoMessage() {}

func (x *NewReduceTask) ProtoReflect() protoreflect.Message {
	mi := &file_coordinator_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use NewReduceTask.ProtoReflect.Descriptor instead.
func (*NewReduceTask) Descriptor() ([]byte, []int) {
	return file_coordinator_proto_rawDescGZIP(), []int{3}
}

func (x *NewReduceTask) GetInputFiles() []string {
	if x != nil {
		return x.InputFiles
	}
	return nil
}

// ReduceTask - message representing ReduceTask
type ReduceTask struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// id - id of the ReduceTask
	Id string `protobuf:"bytes,1,opt,name=id,proto3" json:"id,omitempty"`
	// input_files - input files for ReduceTask
	InputFiles []string `protobuf:"bytes,2,rep,name=input_files,json=inputFiles,proto3" json:"input_files,omitempty"`
}

func (x *ReduceTask) Reset() {
	*x = ReduceTask{}
	if protoimpl.UnsafeEnabled {
		mi := &file_coordinator_proto_msgTypes[4]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ReduceTask) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ReduceTask) ProtoMessage() {}

func (x *ReduceTask) ProtoReflect() protoreflect.Message {
	mi := &file_coordinator_proto_msgTypes[4]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ReduceTask.ProtoReflect.Descriptor instead.
func (*ReduceTask) Descriptor() ([]byte, []int) {
	return file_coordinator_proto_rawDescGZIP(), []int{4}
}

func (x *ReduceTask) GetId() string {
	if x != nil {
		return x.Id
	}
	return ""
}

func (x *ReduceTask) GetInputFiles() []string {
	if x != nil {
		return x.InputFiles
	}
	return nil
}

// ReduceTaskResult - result of ReduceTask execution returned by a Worker
type ReduceTaskResult struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// task_id - id of the ReduceTask
	TaskId string `protobuf:"bytes,1,opt,name=task_id,json=taskId,proto3" json:"task_id,omitempty"`
	// output_file - result of the ReduceTask
	OutputFile string `protobuf:"bytes,2,opt,name=output_file,json=outputFile,proto3" json:"output_file,omitempty"`
	// error - error that have occurred during execution of the ReduceTask by a Worker
	Error *string `protobuf:"bytes,3,opt,name=error,proto3,oneof" json:"error,omitempty"`
}

func (x *ReduceTaskResult) Reset() {
	*x = ReduceTaskResult{}
	if protoimpl.UnsafeEnabled {
		mi := &file_coordinator_proto_msgTypes[5]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ReduceTaskResult) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ReduceTaskResult) ProtoMessage() {}

func (x *ReduceTaskResult) ProtoReflect() protoreflect.Message {
	mi := &file_coordinator_proto_msgTypes[5]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ReduceTaskResult.ProtoReflect.Descriptor instead.
func (*ReduceTaskResult) Descriptor() ([]byte, []int) {
	return file_coordinator_proto_rawDescGZIP(), []int{5}
}

func (x *ReduceTaskResult) GetTaskId() string {
	if x != nil {
		return x.TaskId
	}
	return ""
}

func (x *ReduceTaskResult) GetOutputFile() string {
	if x != nil {
		return x.OutputFile
	}
	return ""
}

func (x *ReduceTaskResult) GetError() string {
	if x != nil && x.Error != nil {
		return *x.Error
	}
	return ""
}

var File_coordinator_proto protoreflect.FileDescriptor

var file_coordinator_proto_rawDesc = []byte{
	0x0a, 0x11, 0x63, 0x6f, 0x6f, 0x72, 0x64, 0x69, 0x6e, 0x61, 0x74, 0x6f, 0x72, 0x2e, 0x70, 0x72,
	0x6f, 0x74, 0x6f, 0x12, 0x0b, 0x63, 0x6f, 0x6f, 0x72, 0x64, 0x69, 0x6e, 0x61, 0x74, 0x6f, 0x72,
	0x1a, 0x1b, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75,
	0x66, 0x2f, 0x65, 0x6d, 0x70, 0x74, 0x79, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x22, 0x2b, 0x0a,
	0x0a, 0x4e, 0x65, 0x77, 0x4d, 0x61, 0x70, 0x54, 0x61, 0x73, 0x6b, 0x12, 0x1d, 0x0a, 0x0a, 0x69,
	0x6e, 0x70, 0x75, 0x74, 0x5f, 0x66, 0x69, 0x6c, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52,
	0x09, 0x69, 0x6e, 0x70, 0x75, 0x74, 0x46, 0x69, 0x6c, 0x65, 0x22, 0x38, 0x0a, 0x07, 0x4d, 0x61,
	0x70, 0x54, 0x61, 0x73, 0x6b, 0x12, 0x0e, 0x0a, 0x02, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28,
	0x09, 0x52, 0x02, 0x69, 0x64, 0x12, 0x1d, 0x0a, 0x0a, 0x69, 0x6e, 0x70, 0x75, 0x74, 0x5f, 0x66,
	0x69, 0x6c, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x09, 0x69, 0x6e, 0x70, 0x75, 0x74,
	0x46, 0x69, 0x6c, 0x65, 0x22, 0x70, 0x0a, 0x0d, 0x4d, 0x61, 0x70, 0x54, 0x61, 0x73, 0x6b, 0x52,
	0x65, 0x73, 0x75, 0x6c, 0x74, 0x12, 0x17, 0x0a, 0x07, 0x74, 0x61, 0x73, 0x6b, 0x5f, 0x69, 0x64,
	0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x06, 0x74, 0x61, 0x73, 0x6b, 0x49, 0x64, 0x12, 0x21,
	0x0a, 0x0c, 0x6f, 0x75, 0x74, 0x70, 0x75, 0x74, 0x5f, 0x66, 0x69, 0x6c, 0x65, 0x73, 0x18, 0x02,
	0x20, 0x03, 0x28, 0x09, 0x52, 0x0b, 0x6f, 0x75, 0x74, 0x70, 0x75, 0x74, 0x46, 0x69, 0x6c, 0x65,
	0x73, 0x12, 0x19, 0x0a, 0x05, 0x65, 0x72, 0x72, 0x6f, 0x72, 0x18, 0x03, 0x20, 0x01, 0x28, 0x09,
	0x48, 0x00, 0x52, 0x05, 0x65, 0x72, 0x72, 0x6f, 0x72, 0x88, 0x01, 0x01, 0x42, 0x08, 0x0a, 0x06,
	0x5f, 0x65, 0x72, 0x72, 0x6f, 0x72, 0x22, 0x30, 0x0a, 0x0d, 0x4e, 0x65, 0x77, 0x52, 0x65, 0x64,
	0x75, 0x63, 0x65, 0x54, 0x61, 0x73, 0x6b, 0x12, 0x1f, 0x0a, 0x0b, 0x69, 0x6e, 0x70, 0x75, 0x74,
	0x5f, 0x66, 0x69, 0x6c, 0x65, 0x73, 0x18, 0x01, 0x20, 0x03, 0x28, 0x09, 0x52, 0x0a, 0x69, 0x6e,
	0x70, 0x75, 0x74, 0x46, 0x69, 0x6c, 0x65, 0x73, 0x22, 0x3d, 0x0a, 0x0a, 0x52, 0x65, 0x64, 0x75,
	0x63, 0x65, 0x54, 0x61, 0x73, 0x6b, 0x12, 0x0e, 0x0a, 0x02, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01,
	0x28, 0x09, 0x52, 0x02, 0x69, 0x64, 0x12, 0x1f, 0x0a, 0x0b, 0x69, 0x6e, 0x70, 0x75, 0x74, 0x5f,
	0x66, 0x69, 0x6c, 0x65, 0x73, 0x18, 0x02, 0x20, 0x03, 0x28, 0x09, 0x52, 0x0a, 0x69, 0x6e, 0x70,
	0x75, 0x74, 0x46, 0x69, 0x6c, 0x65, 0x73, 0x22, 0x71, 0x0a, 0x10, 0x52, 0x65, 0x64, 0x75, 0x63,
	0x65, 0x54, 0x61, 0x73, 0x6b, 0x52, 0x65, 0x73, 0x75, 0x6c, 0x74, 0x12, 0x17, 0x0a, 0x07, 0x74,
	0x61, 0x73, 0x6b, 0x5f, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x06, 0x74, 0x61,
	0x73, 0x6b, 0x49, 0x64, 0x12, 0x1f, 0x0a, 0x0b, 0x6f, 0x75, 0x74, 0x70, 0x75, 0x74, 0x5f, 0x66,
	0x69, 0x6c, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0a, 0x6f, 0x75, 0x74, 0x70, 0x75,
	0x74, 0x46, 0x69, 0x6c, 0x65, 0x12, 0x19, 0x0a, 0x05, 0x65, 0x72, 0x72, 0x6f, 0x72, 0x18, 0x03,
	0x20, 0x01, 0x28, 0x09, 0x48, 0x00, 0x52, 0x05, 0x65, 0x72, 0x72, 0x6f, 0x72, 0x88, 0x01, 0x01,
	0x42, 0x08, 0x0a, 0x06, 0x5f, 0x65, 0x72, 0x72, 0x6f, 0x72, 0x32, 0xfa, 0x03, 0x0a, 0x07, 0x53,
	0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x12, 0x3e, 0x0a, 0x0d, 0x43, 0x72, 0x65, 0x61, 0x74, 0x65,
	0x4d, 0x61, 0x70, 0x54, 0x61, 0x73, 0x6b, 0x12, 0x17, 0x2e, 0x63, 0x6f, 0x6f, 0x72, 0x64, 0x69,
	0x6e, 0x61, 0x74, 0x6f, 0x72, 0x2e, 0x4e, 0x65, 0x77, 0x4d, 0x61, 0x70, 0x54, 0x61, 0x73, 0x6b,
	0x1a, 0x14, 0x2e, 0x63, 0x6f, 0x6f, 0x72, 0x64, 0x69, 0x6e, 0x61, 0x74, 0x6f, 0x72, 0x2e, 0x4d,
	0x61, 0x70, 0x54, 0x61, 0x73, 0x6b, 0x12, 0x47, 0x0a, 0x10, 0x43, 0x72, 0x65, 0x61, 0x74, 0x65,
	0x52, 0x65, 0x64, 0x75, 0x63, 0x65, 0x54, 0x61, 0x73, 0x6b, 0x12, 0x1a, 0x2e, 0x63, 0x6f, 0x6f,
	0x72, 0x64, 0x69, 0x6e, 0x61, 0x74, 0x6f, 0x72, 0x2e, 0x4e, 0x65, 0x77, 0x52, 0x65, 0x64, 0x75,
	0x63, 0x65, 0x54, 0x61, 0x73, 0x6b, 0x1a, 0x17, 0x2e, 0x63, 0x6f, 0x6f, 0x72, 0x64, 0x69, 0x6e,
	0x61, 0x74, 0x6f, 0x72, 0x2e, 0x52, 0x65, 0x64, 0x75, 0x63, 0x65, 0x54, 0x61, 0x73, 0x6b, 0x12,
	0x3a, 0x0a, 0x0a, 0x47, 0x65, 0x74, 0x4d, 0x61, 0x70, 0x54, 0x61, 0x73, 0x6b, 0x12, 0x16, 0x2e,
	0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e,
	0x45, 0x6d, 0x70, 0x74, 0x79, 0x1a, 0x14, 0x2e, 0x63, 0x6f, 0x6f, 0x72, 0x64, 0x69, 0x6e, 0x61,
	0x74, 0x6f, 0x72, 0x2e, 0x4d, 0x61, 0x70, 0x54, 0x61, 0x73, 0x6b, 0x12, 0x40, 0x0a, 0x0d, 0x47,
	0x65, 0x74, 0x52, 0x65, 0x64, 0x75, 0x63, 0x65, 0x54, 0x61, 0x73, 0x6b, 0x12, 0x16, 0x2e, 0x67,
	0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x45,
	0x6d, 0x70, 0x74, 0x79, 0x1a, 0x17, 0x2e, 0x63, 0x6f, 0x6f, 0x72, 0x64, 0x69, 0x6e, 0x61, 0x74,
	0x6f, 0x72, 0x2e, 0x52, 0x65, 0x64, 0x75, 0x63, 0x65, 0x54, 0x61, 0x73, 0x6b, 0x12, 0x49, 0x0a,
	0x13, 0x52, 0x65, 0x70, 0x6f, 0x72, 0x74, 0x4d, 0x61, 0x70, 0x54, 0x61, 0x73, 0x6b, 0x52, 0x65,
	0x73, 0x75, 0x6c, 0x74, 0x12, 0x1a, 0x2e, 0x63, 0x6f, 0x6f, 0x72, 0x64, 0x69, 0x6e, 0x61, 0x74,
	0x6f, 0x72, 0x2e, 0x4d, 0x61, 0x70, 0x54, 0x61, 0x73, 0x6b, 0x52, 0x65, 0x73, 0x75, 0x6c, 0x74,
	0x1a, 0x16, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62,
	0x75, 0x66, 0x2e, 0x45, 0x6d, 0x70, 0x74, 0x79, 0x12, 0x4f, 0x0a, 0x16, 0x52, 0x65, 0x70, 0x6f,
	0x72, 0x74, 0x52, 0x65, 0x64, 0x75, 0x63, 0x65, 0x54, 0x61, 0x73, 0x6b, 0x52, 0x65, 0x73, 0x75,
	0x6c, 0x74, 0x12, 0x1d, 0x2e, 0x63, 0x6f, 0x6f, 0x72, 0x64, 0x69, 0x6e, 0x61, 0x74, 0x6f, 0x72,
	0x2e, 0x52, 0x65, 0x64, 0x75, 0x63, 0x65, 0x54, 0x61, 0x73, 0x6b, 0x52, 0x65, 0x73, 0x75, 0x6c,
	0x74, 0x1a, 0x16, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f,
	0x62, 0x75, 0x66, 0x2e, 0x45, 0x6d, 0x70, 0x74, 0x79, 0x12, 0x4c, 0x0a, 0x1a, 0x46, 0x6c, 0x75,
	0x73, 0x68, 0x43, 0x72, 0x65, 0x61, 0x74, 0x65, 0x64, 0x54, 0x61, 0x73, 0x6b, 0x73, 0x54, 0x6f,
	0x57, 0x6f, 0x72, 0x6b, 0x65, 0x72, 0x73, 0x12, 0x16, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65,
	0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x45, 0x6d, 0x70, 0x74, 0x79, 0x1a,
	0x16, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75,
	0x66, 0x2e, 0x45, 0x6d, 0x70, 0x74, 0x79, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_coordinator_proto_rawDescOnce sync.Once
	file_coordinator_proto_rawDescData = file_coordinator_proto_rawDesc
)

func file_coordinator_proto_rawDescGZIP() []byte {
	file_coordinator_proto_rawDescOnce.Do(func() {
		file_coordinator_proto_rawDescData = protoimpl.X.CompressGZIP(file_coordinator_proto_rawDescData)
	})
	return file_coordinator_proto_rawDescData
}

var file_coordinator_proto_msgTypes = make([]protoimpl.MessageInfo, 6)
var file_coordinator_proto_goTypes = []interface{}{
	(*NewMapTask)(nil),       // 0: coordinator.NewMapTask
	(*MapTask)(nil),          // 1: coordinator.MapTask
	(*MapTaskResult)(nil),    // 2: coordinator.MapTaskResult
	(*NewReduceTask)(nil),    // 3: coordinator.NewReduceTask
	(*ReduceTask)(nil),       // 4: coordinator.ReduceTask
	(*ReduceTaskResult)(nil), // 5: coordinator.ReduceTaskResult
	(*emptypb.Empty)(nil),    // 6: google.protobuf.Empty
}
var file_coordinator_proto_depIdxs = []int32{
	0, // 0: coordinator.Service.CreateMapTask:input_type -> coordinator.NewMapTask
	3, // 1: coordinator.Service.CreateReduceTask:input_type -> coordinator.NewReduceTask
	6, // 2: coordinator.Service.GetMapTask:input_type -> google.protobuf.Empty
	6, // 3: coordinator.Service.GetReduceTask:input_type -> google.protobuf.Empty
	2, // 4: coordinator.Service.ReportMapTaskResult:input_type -> coordinator.MapTaskResult
	5, // 5: coordinator.Service.ReportReduceTaskResult:input_type -> coordinator.ReduceTaskResult
	6, // 6: coordinator.Service.FlushCreatedTasksToWorkers:input_type -> google.protobuf.Empty
	1, // 7: coordinator.Service.CreateMapTask:output_type -> coordinator.MapTask
	4, // 8: coordinator.Service.CreateReduceTask:output_type -> coordinator.ReduceTask
	1, // 9: coordinator.Service.GetMapTask:output_type -> coordinator.MapTask
	4, // 10: coordinator.Service.GetReduceTask:output_type -> coordinator.ReduceTask
	6, // 11: coordinator.Service.ReportMapTaskResult:output_type -> google.protobuf.Empty
	6, // 12: coordinator.Service.ReportReduceTaskResult:output_type -> google.protobuf.Empty
	6, // 13: coordinator.Service.FlushCreatedTasksToWorkers:output_type -> google.protobuf.Empty
	7, // [7:14] is the sub-list for method output_type
	0, // [0:7] is the sub-list for method input_type
	0, // [0:0] is the sub-list for extension type_name
	0, // [0:0] is the sub-list for extension extendee
	0, // [0:0] is the sub-list for field type_name
}

func init() { file_coordinator_proto_init() }
func file_coordinator_proto_init() {
	if File_coordinator_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_coordinator_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*NewMapTask); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_coordinator_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*MapTask); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_coordinator_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*MapTaskResult); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_coordinator_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*NewReduceTask); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_coordinator_proto_msgTypes[4].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ReduceTask); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_coordinator_proto_msgTypes[5].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ReduceTaskResult); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	file_coordinator_proto_msgTypes[2].OneofWrappers = []interface{}{}
	file_coordinator_proto_msgTypes[5].OneofWrappers = []interface{}{}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_coordinator_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   6,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_coordinator_proto_goTypes,
		DependencyIndexes: file_coordinator_proto_depIdxs,
		MessageInfos:      file_coordinator_proto_msgTypes,
	}.Build()
	File_coordinator_proto = out.File
	file_coordinator_proto_rawDesc = nil
	file_coordinator_proto_goTypes = nil
	file_coordinator_proto_depIdxs = nil
}