package job

import "sync"

type executorCache struct {
	executors map[int64][]JobStrategy
	mutex     sync.RWMutex
}

func NewExecutorCache() *executorCache {
	return &executorCache{
		executors: make(map[int64][]JobStrategy),
	}
}

func (receiver *executorCache) register(jobBatchId int64, prototype JobStrategy) {
	receiver.mutex.Lock()
	defer receiver.mutex.Unlock()

	if _, ok := receiver.executors[jobBatchId]; !ok {
		receiver.executors[jobBatchId] = []JobStrategy{prototype}
	} else {
		receiver.executors[jobBatchId] = append(receiver.executors[jobBatchId], prototype)
	}
}

func (receiver *executorCache) delete(jobBatchIds ...int64) {
	receiver.mutex.Lock()
	defer receiver.mutex.Unlock()

	for _, jobBatchId := range jobBatchIds {
		delete(receiver.executors, jobBatchId)
	}
}

func (receiver *executorCache) get(jobBatchId int64) ([]JobStrategy, bool) {
	receiver.mutex.RLock()
	defer receiver.mutex.RUnlock()

	executors, ok := receiver.executors[jobBatchId]
	return executors, ok
}

func (receiver *executorCache) set(jobBatchId int64, strategies []JobStrategy) {
	receiver.mutex.Lock()
	defer receiver.mutex.Unlock()

	receiver.executors[jobBatchId] = strategies
}

func (receiver *executorCache) deleteByNil() {
	receiver.mutex.Lock()
	defer receiver.mutex.Unlock()

	delKeys := make([]int64, 0)
	for key, value := range receiver.executors {
		if allNil(value) {
			delKeys = append(delKeys, key)
		}
	}

	for _, jobBatchId := range delKeys {
		delete(receiver.executors, jobBatchId)
	}
}

func (receiver *executorCache) del(id int64, strategy JobStrategy) {
	receiver.mutex.Lock()
	defer receiver.mutex.Unlock()
	executors, ok := receiver.executors[id]
	if !ok {
		return
	}
	for i, handler := range executors {
		if strategy == handler {
			executors[i] = nil
			break
		}
	}
}

// 判断切片是否全为 nil
func allNil(slice []JobStrategy) bool {
	for _, v := range slice {
		if v != nil {
			return false
		}
	}
	return true
}
