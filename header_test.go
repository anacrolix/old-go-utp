package utp

import (
	"testing"

	"github.com/go-quicktest/qt"
)

func TestSelectiveAckBitmaskBytesLen(t *testing.T) {
	for _, _case := range []struct {
		BitIndex    int
		ExpectedLen int
	}{
		{0, 4},
		{31, 4},
		{32, 8},
	} {
		var selAck selectiveAckBitmask
		selAck.SetBit(_case.BitIndex)
		qt.Check(t, qt.Equals(len(selAck.Bytes), _case.ExpectedLen))
	}
}
