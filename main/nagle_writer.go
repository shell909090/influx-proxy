package main

type NagleWriter {
	writer io.Writer
	lock sync.Mutex
	buffer *bytes.Buffer
	timer *time.Timer
	interval int
}

func (nw *NagleWriter) Flush() {
	nw.lock.Lock()
	defer nw.lock.Unlock()

	p := nw.buffer.Bytes()
	if len(p) == 0 {
		// trigger twice.
		return
	}

	// FIXME: maybe blocked here, should we hold lock now?
	// maybe this can be delayed. Or run in another goroutine?
	n, err := nw.writer.Write(p)
	if err != nil {
		// FIXME: how?
		return
	}
	if n != len(p) {
		// FIXME: why?
		return
	}

	nw.buffer.Reset()
	nw.timer = nil
	return
}

func (nw *NagleWriter) Write(p []byte) (n int, err error) {
	nw.lock.Lock()
	defer nw.lock.Unlock()
	
	n, err = nw.buffer.Write(p)
	if err != nil {
		// FIXME: what?
		return
	}
	if n != len(p) {
		err = io.ErrShortWrite
		return
	}

	// TODO: size > max_size, trigger Flush.
	if nw.timer == nil {
		nw.timer = time.AfterFunc(time.Millisecond * nw.interval, nw.Flush)
	}

	return
}
