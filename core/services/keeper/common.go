package keeper

import (
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"

	evmtypes "github.com/smartcontractkit/chainlink/core/chains/evm/types"
	"github.com/smartcontractkit/chainlink/core/internal/gethwrappers/generated/keeper_registry_wrapper"
)

var RegistryABI = evmtypes.MustGetABI(keeper_registry_wrapper.KeeperRegistryABI)

type Config interface {
	EvmEIP1559DynamicFees() bool
	KeeperDefaultTransactionQueueDepth() uint32
	KeeperGasPriceBufferPercent() uint32
	KeeperGasTipCapBufferPercent() uint32
	KeeperBaseFeeBufferPercent() uint32
	KeeperMaximumGracePeriod() int64
	KeeperRegistryCheckGasOverhead() uint64
	KeeperRegistryPerformGasOverhead() uint64
	KeeperRegistrySyncInterval() time.Duration
	KeeperRegistrySyncUpkeepQueueSize() uint32
	KeeperCheckUpkeepGasPriceFeatureEnabled() bool
	LogSQL() bool
}

//toBinary takes a hash and converts it to a binary string
func toBinary(hash common.Hash) (string, error) {
	big, err := hexutil.DecodeBig(hash.Hex())
	if err != nil {
		return "", err
	}
	binaryString := fmt.Sprintf("%b", big)
	return binaryString, nil
}
