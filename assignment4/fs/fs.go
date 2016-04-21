package fs

import (
	_ "fmt"
	"sync"
	"time"
)

type FileInfo struct {
	filename   string
	contents   []byte
	version    int
	absexptime time.Time
	timer      *time.Timer
}

type FS struct {
	sync.RWMutex
	Dir map[string]*FileInfo
}

// var fs = &FS{dir: make(map[string]*FileInfo, 1000)}
// var gversion = 0 // global version

func (fi *FileInfo) cancelTimer() {
	if fi.timer != nil {
		fi.timer.Stop()
		fi.timer = nil
	}
}

func ProcessMsg(msg *Msg, fs *FS, gversion *int) *Msg {
	switch msg.Kind {
	case 'r':
		return processRead(msg, fs)
	case 'w':
		return processWrite(msg, fs, gversion)
	case 'c':
		return processCas(msg, fs, gversion)
	case 'd':
		return processDelete(msg, fs)
	}

	// Default: Internal error. Shouldn't come here since
	// the msg should have been validated earlier.
	return &Msg{Kind: 'I'}
}

func processRead(msg *Msg, fs *FS) *Msg {
	fs.RLock()
	defer fs.RUnlock()
	if fi := fs.Dir[msg.Filename]; fi != nil {
		remainingTime := 0
		if fi.timer != nil {
			remainingTime := int(fi.absexptime.Sub(time.Now()))
			// fmt.Printf("remaining time: %v \n", remainingTime)
			if remainingTime < 0 {
				remainingTime = 0
			}
		}
		return &Msg{
			Kind:     'C',
			Filename: fi.filename,
			Contents: fi.contents,
			Numbytes: len(fi.contents),
			Exptime:  remainingTime,
			Version:  fi.version,
		}
	} else {
		return &Msg{Kind: 'F'} // file not found
	}
}

func internalWrite(msg *Msg, fs *FS, gversion *int) *Msg {
	fi := fs.Dir[msg.Filename]
	if fi != nil {
		fi.cancelTimer()
	} else {
		fi = &FileInfo{}
	}

	*gversion += 1
	fi.filename = msg.Filename
	fi.contents = msg.Contents
	fi.version = *gversion

	var absexptime time.Time
	if msg.Exptime > 0 {
		dur := time.Duration(msg.Exptime) * time.Second
		// fmt.Printf("file duration: %v \n", dur)
		absexptime = time.Now().Add(dur)
		timerFunc := func(name string, ver int) func() {
			return func() {
				processDelete(&Msg{Kind: 'D',
					Filename: name,
					Version:  ver}, fs)
			}
		}(msg.Filename, *gversion)

		fi.timer = time.AfterFunc(dur, timerFunc)
	}
	fi.absexptime = absexptime
	fs.Dir[msg.Filename] = fi

	return ok(*gversion)
}

func processWrite(msg *Msg, fs *FS, gversion *int) *Msg {
	fs.Lock()
	defer fs.Unlock()
	return internalWrite(msg, fs, gversion)
}

func processCas(msg *Msg, fs *FS, gversion *int) *Msg {
	fs.Lock()
	defer fs.Unlock()

	if fi := fs.Dir[msg.Filename]; fi != nil {
		if msg.Version != fi.version {
			return &Msg{Kind: 'V', Version: fi.version}
		}
	}
	return internalWrite(msg, fs, gversion)
}

func processDelete(msg *Msg, fs *FS) *Msg {
	fs.Lock()
	defer fs.Unlock()
	fi := fs.Dir[msg.Filename]
	if fi != nil {
		if msg.Version > 0 && fi.version != msg.Version {
			// non-zero msg.Version indicates a delete due to an expired timer
			return nil // nothing to do
		}
		fi.cancelTimer()
		delete(fs.Dir, msg.Filename)
		return ok(0)
	} else {
		return &Msg{Kind: 'F'} // file not found
	}

}

func ok(version int) *Msg {
	return &Msg{Kind: 'O', Version: version}
}
