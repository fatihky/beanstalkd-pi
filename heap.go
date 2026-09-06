package main

// Heap is a generic min-heap with pluggable comparator and position callback.
type Heap[T any] struct {
	items  []T
	less   func(a, b T) bool
	setPos func(item T, idx int)
}

func NewHeap[T any](less func(a, b T) bool, setPos func(T, int)) *Heap[T] {
	return &Heap[T]{
		less:   less,
		setPos: setPos,
	}
}

func (h *Heap[T]) Len() int { return len(h.items) }

func (h *Heap[T]) Less(i, j int) bool { return h.less(h.items[i], h.items[j]) }

func (h *Heap[T]) Push(item T) {
	h.items = append(h.items, item)
	if h.setPos != nil {
		h.setPos(item, len(h.items)-1)
	}
	h.siftDown(len(h.items) - 1)
}

func (h *Heap[T]) Pop() T {
	n := len(h.items)
	var zero T
	if n == 0 {
		return zero
	}
	top := h.items[0]
	if h.setPos != nil {
		h.setPos(top, -1)
	}
	if n == 1 {
		h.items = h.items[:0]
		return top
	}
	last := h.items[n-1]
	h.items[0] = last
	if h.setPos != nil {
		h.setPos(last, 0)
	}
	h.items = h.items[:n-1]
	h.siftUp(0)
	return top
}

func (h *Heap[T]) Peek() T {
	var zero T
	if len(h.items) == 0 {
		return zero
	}
	return h.items[0]
}

func (h *Heap[T]) Remove(idx int) {
	n := len(h.items)
	if idx < 0 || idx >= n {
		return
	}
	item := h.items[idx]
	if h.setPos != nil {
		h.setPos(item, -1)
	}
	if idx == n-1 {
		h.items = h.items[:n-1]
		return
	}
	last := h.items[n-1]
	h.items[idx] = last
	if h.setPos != nil {
		h.setPos(last, idx)
	}
	h.items = h.items[:n-1]
	h.siftDown(idx)
	h.siftUp(idx)
}

func (h *Heap[T]) Fix(idx int) {
	if idx < 0 || idx >= len(h.items) {
		return
	}
	h.siftDown(idx)
	h.siftUp(idx)
}

// siftDown moves element toward root while smaller than parent.
func (h *Heap[T]) siftDown(k int) {
	for k > 0 {
		parent := (k - 1) / 2
		if !h.Less(k, parent) {
			break
		}
		h.swap(k, parent)
		k = parent
	}
}

// siftUp moves element toward leaves while larger than smallest child.
func (h *Heap[T]) siftUp(k int) {
	n := len(h.items)
	for {
		smallest := k
		left := 2*k + 1
		right := 2*k + 2
		if left < n && h.Less(left, smallest) {
			smallest = left
		}
		if right < n && h.Less(right, smallest) {
			smallest = right
		}
		if smallest == k {
			break
		}
		h.swap(k, smallest)
		k = smallest
	}
}

func (h *Heap[T]) swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
	if h.setPos != nil {
		h.setPos(h.items[i], i)
		h.setPos(h.items[j], j)
	}
}
