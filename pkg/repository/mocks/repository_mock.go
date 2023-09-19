// Code generated by MockGen. DO NOT EDIT.
// Source: repository.go

// Package mocks is a generated GoMock package.
package mocks

import (
	context "context"
	reflect "reflect"

	gomock "github.com/golang/mock/gomock"
	mapreduce "github.com/mtvarkovsky/go-mapreduce/pkg/mapreduce"
	repository "github.com/mtvarkovsky/go-mapreduce/pkg/repository"
)

// MockTransaction is a mock of Transaction interface.
type MockTransaction struct {
	ctrl     *gomock.Controller
	recorder *MockTransactionMockRecorder
}

// MockTransactionMockRecorder is the mock recorder for MockTransaction.
type MockTransactionMockRecorder struct {
	mock *MockTransaction
}

// NewMockTransaction creates a new mock instance.
func NewMockTransaction(ctrl *gomock.Controller) *MockTransaction {
	mock := &MockTransaction{ctrl: ctrl}
	mock.recorder = &MockTransactionMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockTransaction) EXPECT() *MockTransactionMockRecorder {
	return m.recorder
}

// Transaction mocks base method.
func (m *MockTransaction) Transaction(ctx context.Context, transaction func(context.Context) (any, error)) (any, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Transaction", ctx, transaction)
	ret0, _ := ret[0].(any)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Transaction indicates an expected call of Transaction.
func (mr *MockTransactionMockRecorder) Transaction(ctx, transaction interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Transaction", reflect.TypeOf((*MockTransaction)(nil).Transaction), ctx, transaction)
}

// MockTasks is a mock of Tasks interface.
type MockTasks struct {
	ctrl     *gomock.Controller
	recorder *MockTasksMockRecorder
}

// MockTasksMockRecorder is the mock recorder for MockTasks.
type MockTasksMockRecorder struct {
	mock *MockTasks
}

// NewMockTasks creates a new mock instance.
func NewMockTasks(ctrl *gomock.Controller) *MockTasks {
	mock := &MockTasks{ctrl: ctrl}
	mock.recorder = &MockTasksMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockTasks) EXPECT() *MockTasksMockRecorder {
	return m.recorder
}

// CreateMapTask mocks base method.
func (m *MockTasks) CreateMapTask(ctx context.Context, task mapreduce.MapTask) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CreateMapTask", ctx, task)
	ret0, _ := ret[0].(error)
	return ret0
}

// CreateMapTask indicates an expected call of CreateMapTask.
func (mr *MockTasksMockRecorder) CreateMapTask(ctx, task interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CreateMapTask", reflect.TypeOf((*MockTasks)(nil).CreateMapTask), ctx, task)
}

// CreateReduceTask mocks base method.
func (m *MockTasks) CreateReduceTask(ctx context.Context, task mapreduce.ReduceTask) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CreateReduceTask", ctx, task)
	ret0, _ := ret[0].(error)
	return ret0
}

// CreateReduceTask indicates an expected call of CreateReduceTask.
func (mr *MockTasksMockRecorder) CreateReduceTask(ctx, task interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CreateReduceTask", reflect.TypeOf((*MockTasks)(nil).CreateReduceTask), ctx, task)
}

// GetMapTask mocks base method.
func (m *MockTasks) GetMapTask(ctx context.Context, id string) (mapreduce.MapTask, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetMapTask", ctx, id)
	ret0, _ := ret[0].(mapreduce.MapTask)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetMapTask indicates an expected call of GetMapTask.
func (mr *MockTasksMockRecorder) GetMapTask(ctx, id interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetMapTask", reflect.TypeOf((*MockTasks)(nil).GetMapTask), ctx, id)
}

// GetReduceTask mocks base method.
func (m *MockTasks) GetReduceTask(ctx context.Context, id string) (mapreduce.ReduceTask, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetReduceTask", ctx, id)
	ret0, _ := ret[0].(mapreduce.ReduceTask)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetReduceTask indicates an expected call of GetReduceTask.
func (mr *MockTasksMockRecorder) GetReduceTask(ctx, id interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetReduceTask", reflect.TypeOf((*MockTasks)(nil).GetReduceTask), ctx, id)
}

// QueryMapTasks mocks base method.
func (m *MockTasks) QueryMapTasks(ctx context.Context, filter repository.Filter) ([]mapreduce.MapTask, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "QueryMapTasks", ctx, filter)
	ret0, _ := ret[0].([]mapreduce.MapTask)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// QueryMapTasks indicates an expected call of QueryMapTasks.
func (mr *MockTasksMockRecorder) QueryMapTasks(ctx, filter interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "QueryMapTasks", reflect.TypeOf((*MockTasks)(nil).QueryMapTasks), ctx, filter)
}

// QueryReduceTasks mocks base method.
func (m *MockTasks) QueryReduceTasks(ctx context.Context, filter repository.Filter) ([]mapreduce.ReduceTask, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "QueryReduceTasks", ctx, filter)
	ret0, _ := ret[0].([]mapreduce.ReduceTask)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// QueryReduceTasks indicates an expected call of QueryReduceTasks.
func (mr *MockTasksMockRecorder) QueryReduceTasks(ctx, filter interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "QueryReduceTasks", reflect.TypeOf((*MockTasks)(nil).QueryReduceTasks), ctx, filter)
}

// Transaction mocks base method.
func (m *MockTasks) Transaction(ctx context.Context, transaction func(context.Context) (any, error)) (any, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Transaction", ctx, transaction)
	ret0, _ := ret[0].(any)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Transaction indicates an expected call of Transaction.
func (mr *MockTasksMockRecorder) Transaction(ctx, transaction interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Transaction", reflect.TypeOf((*MockTasks)(nil).Transaction), ctx, transaction)
}

// UpdateMapTask mocks base method.
func (m *MockTasks) UpdateMapTask(ctx context.Context, task mapreduce.MapTask) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "UpdateMapTask", ctx, task)
	ret0, _ := ret[0].(error)
	return ret0
}

// UpdateMapTask indicates an expected call of UpdateMapTask.
func (mr *MockTasksMockRecorder) UpdateMapTask(ctx, task interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UpdateMapTask", reflect.TypeOf((*MockTasks)(nil).UpdateMapTask), ctx, task)
}

// UpdateMapTasks mocks base method.
func (m *MockTasks) UpdateMapTasks(ctx context.Context, ids []string, fields repository.UpdateFields) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "UpdateMapTasks", ctx, ids, fields)
	ret0, _ := ret[0].(error)
	return ret0
}

// UpdateMapTasks indicates an expected call of UpdateMapTasks.
func (mr *MockTasksMockRecorder) UpdateMapTasks(ctx, ids, fields interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UpdateMapTasks", reflect.TypeOf((*MockTasks)(nil).UpdateMapTasks), ctx, ids, fields)
}

// UpdateReduceTask mocks base method.
func (m *MockTasks) UpdateReduceTask(ctx context.Context, task mapreduce.ReduceTask) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "UpdateReduceTask", ctx, task)
	ret0, _ := ret[0].(error)
	return ret0
}

// UpdateReduceTask indicates an expected call of UpdateReduceTask.
func (mr *MockTasksMockRecorder) UpdateReduceTask(ctx, task interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UpdateReduceTask", reflect.TypeOf((*MockTasks)(nil).UpdateReduceTask), ctx, task)
}

// UpdateReduceTasks mocks base method.
func (m *MockTasks) UpdateReduceTasks(ctx context.Context, ids []string, fields repository.UpdateFields) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "UpdateReduceTasks", ctx, ids, fields)
	ret0, _ := ret[0].(error)
	return ret0
}

// UpdateReduceTasks indicates an expected call of UpdateReduceTasks.
func (mr *MockTasksMockRecorder) UpdateReduceTasks(ctx, ids, fields interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UpdateReduceTasks", reflect.TypeOf((*MockTasks)(nil).UpdateReduceTasks), ctx, ids, fields)
}