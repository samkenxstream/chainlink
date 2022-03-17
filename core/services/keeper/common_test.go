package keeper

import (
	"fmt"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/assert"
)

func Test_ToBinary_Success(t *testing.T) {
	t.Parallel()
	hash := common.HexToHash("0xb39b598da7b14813677c0402f702d96b1a71516bbb5d39457eaa82d409f388b9")
	binary, err := toBinary(hash)
	assert.NoError(t, err, "shouldn't have errors")
	assert.Equal(t, "1011001110011011010110011000110110100111101100010100100000010011011001110111110000000100000000101111011100000010110110010110101100011010011100010101000101101011101110110101110100111001010001010111111010101010100000101101010000001001111100111000100010111001",
		binary, "binary match")
}

func Test_ToBinary_ErrorZeroHex(t *testing.T) {
	t.Parallel()
	hash := common.HexToHash("0x00000000000000000000")
	b, err := toBinary(hash)
	fmt.Println(b)
	assert.Error(t, err, "hex number with leading zero digits")
}
