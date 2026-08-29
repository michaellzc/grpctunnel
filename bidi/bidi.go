//
// Copyright 2019 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

// Package bidi provides a method to bidirectionally copy between two readwriteclosers.
package bidi

import (
	"io"
)

type closeWriter interface {
	CloseWrite() error
}

// Copy starts bi-directional copying between two read write closers.
func Copy(i, j io.ReadWriteCloser) error {
	errCh := make(chan error, 2)

	copy := func(dst, src io.ReadWriteCloser) {
		_, err := io.Copy(dst, src)
		if err == nil || err == io.EOF {
			if w, ok := dst.(closeWriter); ok {
				err = w.CloseWrite()
			} else {
				err = dst.Close()
			}
		}
		errCh <- err
	}

	go copy(i, j)
	go copy(j, i)

	firstErr := <-errCh
	if firstErr != nil && firstErr != io.EOF {
		// A terminal error in either direction must tear down both sides. Do not
		// wait for the second copy: returning lets an owning gRPC handler exit and
		// cancel a Recv that cannot otherwise be interrupted from server code.
		_ = i.Close()
		_ = j.Close()
		return firstErr
	}

	secondErr := <-errCh
	_ = i.Close()
	_ = j.Close()
	if secondErr == io.EOF {
		return nil
	}
	return secondErr
}
