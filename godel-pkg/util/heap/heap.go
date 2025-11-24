/*
Copyright 2019 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Below is the implementation of the a heap. The logic is pretty much the same
// as cache.heap, however, this heap does not perform synchronization. It leaves
// synchronization to the SchedulingQueue.

package heap

import (
	"container/heap"
	"context"
	"fmt"
	"time"

	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"

	"k8s.io/kubernetes/godel-pkg/common/metrics"
	"k8s.io/kubernetes/godel-pkg/util/parallelize"
)

// KeyFunc is a function type to get the key from an object.
type KeyFunc func(obj interface{}) (string, error)

// ProcessFunc is a function type to process each item in parallel.
// It MUST be a read-only function.
type ProcessFunc func(index int, key string, obj interface{})

type heapItem struct {
	obj   interface{} // The object which is stored in the heap.
	index int         // The index of the object's key in the Heap.queue.
}

type itemKeyValue struct {
	key string
	obj interface{}
}

// data is an internal struct that implements the standard heap interface
// and keeps the data stored in the heap.
type data struct {
	// items is a map from key of the objects to the objects and their index.
	// We depend on the property that items in the map are in the queue and vice versa.
	items map[string]*heapItem
	// queue implements a heap data structure and keeps the order of elements
	// according to the heap invariant. The queue keeps the keys of objects stored
	// in "items".
	queue []string

	// keyFunc is used to make the key used for queued item insertion and retrieval, and
	// should be deterministic.
	keyFunc KeyFunc
	// lessFunc is used to compare two objects in the heap.
	lessFunc lessFunc
}

var _ = heap.Interface(&data{}) // heapData is a standard heap

// Less compares two objects and returns true if the first one should go
// in front of the second one in the heap.
func (h *data) Less(i, j int) bool {
	if i > len(h.queue) || j > len(h.queue) {
		return false
	}
	itemi, ok := h.items[h.queue[i]]
	if !ok {
		return false
	}
	itemj, ok := h.items[h.queue[j]]
	if !ok {
		return false
	}
	return h.lessFunc(itemi.obj, itemj.obj)
}

// Len returns the number of items in the Heap.
func (h *data) Len() int { return len(h.queue) }

// Swap implements swapping of two elements in the heap. This is a part of standard
// heap interface and should never be called directly.
func (h *data) Swap(i, j int) {
	h.queue[i], h.queue[j] = h.queue[j], h.queue[i]
	item := h.items[h.queue[i]]
	item.index = i
	item = h.items[h.queue[j]]
	item.index = j
}

// Push is supposed to be called by heap.Push only.
func (h *data) Push(kv interface{}) {
	klog.Info("调用heap的push函数")
	keyValue := kv.(*itemKeyValue)
	n := len(h.queue)
	h.items[keyValue.key] = &heapItem{keyValue.obj, n}
	h.queue = append(h.queue, keyValue.key)
}

// Pop is supposed to be called by heap.Pop only.
func (h *data) Pop() interface{} {
	key := h.queue[len(h.queue)-1]
	h.queue = h.queue[0 : len(h.queue)-1]
	item, ok := h.items[key]
	if !ok {
		// This is an error
		return nil
	}
	delete(h.items, key)
	return item.obj
}

// Peek is supposed to be called by heap.Peek only.
func (h *data) Peek() interface{} {
	if len(h.queue) > 0 {
		return h.items[h.queue[0]].obj
	}
	return nil
}

// Heap is a producer/consumer queue that implements a heap data structure.
// It can be used to implement priority queues and similar data structures.
type Heap struct {
	name string
	// data stores objects and has a queue that keeps their ordering according
	// to the heap invariant.
	data *data
	// metricRecorder updates the counter when elements of a heap get added or
	// removed, and it does nothing if it's nil
	metricRecorder metrics.MetricRecorder
}

// Add 将一个对象插入到堆（Heap）中，如果该对象已存在则更新它。
// 该函数会计算对象的键，检查对象是否已存在于堆中，
// 如果存在则更新对象值并修复堆结构，如果不存在则将新对象添加到堆中。
// 这是 Heap 数据结构的核心插入方法，用于维护有序的堆结构。
func (h *Heap) Add(obj interface{}) error {
	// 记录日志，表示开始执行 Heap 的 Add 操作
	klog.Info("启动heap的add函数")

	// 使用键函数计算对象的唯一键值
	key, err := h.data.keyFunc(obj)
	if err != nil {
		// 如果键计算失败，返回键错误
		return cache.KeyError{Obj: obj, Err: err}
	}
	// 打印计算出的键值
	klog.InfoS("Heap Add operation", "key", key, "objType", fmt.Sprintf("%T", obj))
	// 记录操作开始时间，用于性能监控
	start := time.Now()

	// 检查对象是否已存在于堆中
	if _, exists := h.data.items[key]; exists {
		// 如果对象已存在，更新现有对象的值
		klog.Info("对象已经存在")
		h.data.items[key].obj = obj
		// 修复堆结构以保持堆的有序性质
		heap.Fix(h.data, h.data.items[key].index)
	} else {
		klog.Info("对象不存在")
		// 如果对象不存在，创建新的项并将其推入堆中
		heap.Push(h.data, &itemKeyValue{key, obj})
		// 如果指标记录器存在，增加对象计数指标
		if h.metricRecorder != nil {
			h.metricRecorder.Inc(obj)
		}
	}

	// 如果指标记录器存在，记录添加操作的延迟时间
	if h.metricRecorder != nil {
		h.metricRecorder.AddingLatencyInSeconds(obj, time.Since(start).Seconds())
	}

	return nil
}

// AddIfNotPresent inserts an item, and puts it in the queue. If an item with
// the key is present in the map, no changes is made to the item.
func (h *Heap) AddIfNotPresent(obj interface{}) error {
	key, err := h.data.keyFunc(obj)
	if err != nil {
		return cache.KeyError{Obj: obj, Err: err}
	}
	if _, exists := h.data.items[key]; !exists {
		heap.Push(h.data, &itemKeyValue{key, obj})
		if h.metricRecorder != nil {
			h.metricRecorder.Inc(obj)
		}
	}
	return nil
}

// Update is the same as Add in this implementation. When the item does not
// exist, it is added.
func (h *Heap) Update(oldObj, newObj interface{}) error {
	if oldObj != nil {
		h.Delete(oldObj)
	}
	return h.Add(newObj)
}

// Delete removes an item.
func (h *Heap) Delete(obj interface{}) error {
	key, err := h.data.keyFunc(obj)
	if err != nil {
		return cache.KeyError{Obj: obj, Err: err}
	}
	return h.DeleteByKey(key)
}

// DeleteByKey removes an item by key.
func (h *Heap) DeleteByKey(key string) error {
	if item, ok := h.data.items[key]; ok {
		heap.Remove(h.data, item.index)
		if h.metricRecorder != nil {
			h.metricRecorder.Dec(item.obj)
		}
		return nil
	}
	return fmt.Errorf("object not found")
}

// Peek returns the head of the heap without removing it.
func (h *Heap) Peek() interface{} {
	return h.data.Peek()
}

// Pop returns the head of the heap and removes it.
func (h *Heap) Pop() (interface{}, error) {
	obj := heap.Pop(h.data)
	if obj != nil {
		if h.metricRecorder != nil {
			h.metricRecorder.Dec(obj)
		}
		return obj, nil
	}
	return nil, fmt.Errorf("object was removed from heap data")
}

// Get returns the requested item, or sets exists=false.
func (h *Heap) Get(obj interface{}) (interface{}, bool, error) {
	key, err := h.data.keyFunc(obj)
	if err != nil {
		return nil, false, cache.KeyError{Obj: obj, Err: err}
	}
	return h.GetByKey(key)
}

// GetByKey returns the requested item, or sets exists=false.
func (h *Heap) GetByKey(key string) (interface{}, bool, error) {
	item, exists := h.data.items[key]
	if !exists {
		return nil, false, nil
	}
	return item.obj, true, nil
}

// List returns a list of all the items.
func (h *Heap) List() []interface{} {
	list := make([]interface{}, 0, len(h.data.items))
	for _, item := range h.data.items {
		list = append(list, item.obj)
	}
	return list
}

// Len returns the number of items in the heap.
func (h *Heap) Len() int {
	return len(h.data.queue)
}

func (h *Heap) String() string {
	return h.name
}

// `f` must be a read-only function
func (h *Heap) Process(f ProcessFunc) {
	parallelize.Until(context.Background(), len(h.data.queue), func(i int) {
		f(i, h.data.queue[i], h.data.items[h.data.queue[i]].obj)
	})
}

// New returns a Heap which can be used to queue up items to process.
func New(name string, keyFn KeyFunc, lessFn lessFunc) *Heap {
	return NewWithRecorder(name, keyFn, lessFn, nil)
}

// NewWithRecorder wraps an optional metricRecorder to compose a Heap object.
func NewWithRecorder(name string, keyFn KeyFunc, lessFn lessFunc, metricRecorder metrics.MetricRecorder) *Heap {
	return &Heap{
		name: name,
		data: &data{
			items:    map[string]*heapItem{},
			queue:    []string{},
			keyFunc:  keyFn,
			lessFunc: lessFn,
		},
		metricRecorder: metricRecorder,
	}
}

// lessFunc is a function that receives two items and returns true if the first
// item should be placed before the second one when the list is sorted.
type lessFunc = func(item1, item2 interface{}) bool
