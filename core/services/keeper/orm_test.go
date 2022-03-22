package keeper_test

import (
	"math/big"
	"sort"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/onsi/gomega"
	"github.com/smartcontractkit/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	evmconfig "github.com/smartcontractkit/chainlink/core/chains/evm/config"
	"github.com/smartcontractkit/chainlink/core/chains/evm/txmgr"
	evmtypes "github.com/smartcontractkit/chainlink/core/chains/evm/types"
	"github.com/smartcontractkit/chainlink/core/internal/cltest"
	"github.com/smartcontractkit/chainlink/core/internal/testutils/evmtest"
	"github.com/smartcontractkit/chainlink/core/internal/testutils/pgtest"
	"github.com/smartcontractkit/chainlink/core/logger"
	"github.com/smartcontractkit/chainlink/core/services/keeper"
	"github.com/smartcontractkit/chainlink/core/utils"
)

var (
	checkData  = common.Hex2Bytes("ABC123")
	executeGas = uint64(10_000)
)

func setupKeeperDB(t *testing.T) (
	*sqlx.DB,
	evmconfig.ChainScopedConfig,
	keeper.ORM,
) {
	gcfg := cltest.NewTestGeneralConfig(t)
	db := pgtest.NewSqlxDB(t)
	cfg := evmtest.NewChainScopedConfig(t, gcfg)
	orm := keeper.NewORM(db, logger.TestLogger(t), cfg, txmgr.SendEveryStrategy{})
	return db, cfg, orm
}

func newUpkeep(registry keeper.Registry, upkeepID int64) keeper.UpkeepRegistration {
	return keeper.UpkeepRegistration{
		UpkeepID:   upkeepID,
		ExecuteGas: executeGas,
		Registry:   registry,
		RegistryID: registry.ID,
		CheckData:  checkData,
	}
}

func waitLastRunHeight(t *testing.T, db *sqlx.DB, upkeep keeper.UpkeepRegistration, height int64) {
	t.Helper()

	gomega.NewWithT(t).Eventually(func() int64 {
		err := db.Get(&upkeep, `SELECT * FROM upkeep_registrations WHERE id = $1`, upkeep.ID)
		require.NoError(t, err)
		return upkeep.LastRunBlockHeight
	}, time.Second*2, time.Millisecond*100).Should(gomega.Equal(height))
}

func assertLastRunHeight(t *testing.T, db *sqlx.DB, upkeep keeper.UpkeepRegistration, lastRunBlockHeight int64, lastKeeperIndex int64) {
	err := db.Get(&upkeep, `SELECT * FROM upkeep_registrations WHERE id = $1`, upkeep.ID)
	require.NoError(t, err)
	require.Equal(t, lastRunBlockHeight, upkeep.LastRunBlockHeight)
	require.Equal(t, lastKeeperIndex, upkeep.LastKeeperIndex.Int64)
}

func TestKeeperDB_Registries(t *testing.T) {
	t.Parallel()
	db, config, orm := setupKeeperDB(t)
	ethKeyStore := cltest.NewKeyStore(t, db, config).Eth()

	cltest.MustInsertKeeperRegistry(t, db, orm, ethKeyStore, 0, 1, 20)
	cltest.MustInsertKeeperRegistry(t, db, orm, ethKeyStore, 0, 1, 20)

	existingRegistries, err := orm.Registries()
	require.NoError(t, err)
	require.Equal(t, 2, len(existingRegistries))
}

func TestKeeperDB_UpsertUpkeep(t *testing.T) {
	t.Parallel()
	db, config, orm := setupKeeperDB(t)
	ethKeyStore := cltest.NewKeyStore(t, db, config).Eth()

	registry, _ := cltest.MustInsertKeeperRegistry(t, db, orm, ethKeyStore, 0, 1, 20)
	upkeep := keeper.UpkeepRegistration{
		UpkeepID:            0,
		ExecuteGas:          executeGas,
		Registry:            registry,
		RegistryID:          registry.ID,
		CheckData:           checkData,
		LastRunBlockHeight:  1,
		PositioningConstant: 1,
	}
	require.NoError(t, orm.UpsertUpkeep(&upkeep))
	cltest.AssertCount(t, db, "upkeep_registrations", 1)

	// update upkeep
	upkeep.ExecuteGas = 20_000
	upkeep.CheckData = common.Hex2Bytes("8888")
	upkeep.PositioningConstant = 2
	upkeep.LastRunBlockHeight = 2

	err := orm.UpsertUpkeep(&upkeep)
	require.NoError(t, err)
	cltest.AssertCount(t, db, "upkeep_registrations", 1)

	var upkeepFromDB keeper.UpkeepRegistration
	err = db.Get(&upkeepFromDB, `SELECT * FROM upkeep_registrations ORDER BY id LIMIT 1`)
	require.NoError(t, err)
	require.Equal(t, uint64(20_000), upkeepFromDB.ExecuteGas)
	require.Equal(t, "8888", common.Bytes2Hex(upkeepFromDB.CheckData))
	require.Equal(t, int32(2), upkeepFromDB.PositioningConstant)
	require.Equal(t, int64(1), upkeepFromDB.LastRunBlockHeight) // shouldn't change on upsert
}

func TestKeeperDB_BatchDeleteUpkeepsForJob(t *testing.T) {
	t.Parallel()
	db, config, orm := setupKeeperDB(t)
	ethKeyStore := cltest.NewKeyStore(t, db, config).Eth()

	registry, job := cltest.MustInsertKeeperRegistry(t, db, orm, ethKeyStore, 0, 1, 20)

	for i := int64(0); i < 3; i++ {
		cltest.MustInsertUpkeepForRegistry(t, db, config, registry)
	}

	cltest.AssertCount(t, db, "upkeep_registrations", 3)

	_, err := orm.BatchDeleteUpkeepsForJob(job.ID, []int64{0, 2})
	require.NoError(t, err)
	cltest.AssertCount(t, db, "upkeep_registrations", 1)

	var remainingUpkeep keeper.UpkeepRegistration
	err = db.Get(&remainingUpkeep, `SELECT * FROM upkeep_registrations ORDER BY id LIMIT 1`)
	require.NoError(t, err)
	require.Equal(t, int64(1), remainingUpkeep.UpkeepID)
}

func TestKeeperDB_EligibleUpkeeps_Shuffle(t *testing.T) {
	t.Parallel()
	db, config, orm := setupKeeperDB(t)
	ethKeyStore := cltest.NewKeyStore(t, db, config).Eth()

	blockheight := int64(63)
	gracePeriod := int64(10)

	registry, _ := cltest.MustInsertKeeperRegistry(t, db, orm, ethKeyStore, 0, 1, 20)

	head := evmtypes.NewHead(big.NewInt(blockheight), utils.NewHash(), utils.NewHash(), 1000, utils.NewBigI(0))
	putTurnBlockAsParent(&head, registry)

	ordered := [100]int64{}
	for i := 0; i < 100; i++ {
		k := newUpkeep(registry, int64(i))
		ordered[i] = int64(i)
		err := orm.UpsertUpkeep(&k)
		require.NoError(t, err)
	}

	cltest.AssertCount(t, db, "upkeep_registrations", 100)

	eligibleUpkeeps, err := orm.EligibleUpkeepsForRegistry(registry.ContractAddress, &head, gracePeriod)
	assert.NoError(t, err)

	require.Len(t, eligibleUpkeeps, 100)
	shuffled := [100]int64{}
	for i := 0; i < 100; i++ {
		shuffled[i] = eligibleUpkeeps[i].UpkeepID
	}
	assert.NotEqualValues(t, ordered, shuffled)
}

func TestKeeperDB_EligibleUpkeeps_BlockCountPerTurn(t *testing.T) {
	t.Parallel()
	db, config, orm := setupKeeperDB(t)
	ethKeyStore := cltest.NewKeyStore(t, db, config).Eth()

	blockheight := int64(63)
	gracePeriod := int64(10)

	registry, _ := cltest.MustInsertKeeperRegistry(t, db, orm, ethKeyStore, 0, 1, 20)
	head := evmtypes.NewHead(big.NewInt(blockheight), utils.NewHash(), utils.NewHash(), 1000, utils.NewBigI(0))
	putTurnBlockAsParent(&head, registry)

	upkeeps := [5]keeper.UpkeepRegistration{
		newUpkeep(registry, 0),
		newUpkeep(registry, 1),
		newUpkeep(registry, 2),
		newUpkeep(registry, 3),
		newUpkeep(registry, 4),
	}

	upkeeps[0].LastRunBlockHeight = 0  // Never run
	upkeeps[1].LastRunBlockHeight = 41 // Run last turn, outside grade period
	upkeeps[2].LastRunBlockHeight = 46 // Run last turn, outside grade period
	upkeeps[3].LastRunBlockHeight = 59 // Run last turn, inside grace period (EXCLUDE)
	upkeeps[4].LastRunBlockHeight = 61 // Run this turn, inside grace period (EXCLUDE)

	for i := range upkeeps {
		err := orm.UpsertUpkeep(&upkeeps[i])
		require.NoError(t, err)
	}

	cltest.AssertCount(t, db, "upkeep_registrations", 5)

	eligibleUpkeeps, err := orm.EligibleUpkeepsForRegistry(registry.ContractAddress, &head, gracePeriod)
	assert.NoError(t, err)

	// 3 out of 5 are eligible, check that ids are 0,1 or 2 but order is shuffled so can not use equals
	require.Len(t, eligibleUpkeeps, 3)
	assert.Less(t, eligibleUpkeeps[0].UpkeepID, int64(3))
	assert.Less(t, eligibleUpkeeps[1].UpkeepID, int64(3))
	assert.Less(t, eligibleUpkeeps[2].UpkeepID, int64(3))

	// preloads registry data
	assert.Equal(t, registry.ID, eligibleUpkeeps[0].RegistryID)
	assert.Equal(t, registry.ID, eligibleUpkeeps[1].RegistryID)
	assert.Equal(t, registry.ID, eligibleUpkeeps[2].RegistryID)
	assert.Equal(t, registry.CheckGas, eligibleUpkeeps[0].Registry.CheckGas)
	assert.Equal(t, registry.CheckGas, eligibleUpkeeps[1].Registry.CheckGas)
	assert.Equal(t, registry.CheckGas, eligibleUpkeeps[2].Registry.CheckGas)
	assert.Equal(t, registry.ContractAddress, eligibleUpkeeps[0].Registry.ContractAddress)
	assert.Equal(t, registry.ContractAddress, eligibleUpkeeps[1].Registry.ContractAddress)
	assert.Equal(t, registry.ContractAddress, eligibleUpkeeps[2].Registry.ContractAddress)
}

func TestKeeperDB_EligibleUpkeeps_GracePeriod(t *testing.T) {
	t.Parallel()
	db, config, orm := setupKeeperDB(t)
	ethKeyStore := cltest.NewKeyStore(t, db, config).Eth()

	blockheight := int64(120)
	gracePeriod := int64(100)

	registry, _ := cltest.MustInsertKeeperRegistry(t, db, orm, ethKeyStore, 0, 1, 20)
	head := evmtypes.NewHead(big.NewInt(blockheight), utils.NewHash(), utils.NewHash(), 1000, utils.NewBigI(0))
	putTurnBlockAsParent(&head, registry)

	upkeep1 := newUpkeep(registry, 0)
	upkeep1.LastRunBlockHeight = 0
	upkeep2 := newUpkeep(registry, 1)
	upkeep2.LastRunBlockHeight = 19
	upkeep3 := newUpkeep(registry, 2)
	upkeep3.LastRunBlockHeight = 20

	upkeepRegistrations := [3]keeper.UpkeepRegistration{upkeep1, upkeep2, upkeep3}
	for i := range upkeepRegistrations {
		err := orm.UpsertUpkeep(&upkeepRegistrations[i])
		require.NoError(t, err)
	}

	cltest.AssertCount(t, db, "upkeep_registrations", 3)

	eligibleUpkeeps, err := orm.EligibleUpkeepsForRegistry(registry.ContractAddress, &head, gracePeriod)
	assert.NoError(t, err)
	// 2 out of 3 are eligible, check that ids are 0 or 1 but order is shuffled so can not use equals
	assert.Len(t, eligibleUpkeeps, 2)
	assert.Less(t, eligibleUpkeeps[0].UpkeepID, int64(2))
	assert.Less(t, eligibleUpkeeps[1].UpkeepID, int64(2))
}

func TestKeeperDB_EligibleUpkeeps_TurnsRandom(t *testing.T) {
	t.Parallel()
	db, config, orm := setupKeeperDB(t)
	ethKeyStore := cltest.NewKeyStore(t, db, config).Eth()

	registry, _ := cltest.MustInsertKeeperRegistry(t, db, orm, ethKeyStore, 0, 3, 10)

	for i := 0; i < 10; i++ {
		cltest.MustInsertUpkeepForRegistry(t, db, config, registry)
	}

	cltest.AssertCount(t, db, "keeper_registries", 1)
	cltest.AssertCount(t, db, "upkeep_registrations", 10)

	// 3 keepers 10 block turns would make 20 and 50 the same bucket via the old way but now should be different every turn
	h1 := evmtypes.NewHead(big.NewInt(20), utils.NewHash(), utils.NewHash(), 1000, utils.NewBigI(0))
	putTurnBlockAsParent(&h1, registry)
	list1, err := orm.EligibleUpkeepsForRegistry(registry.ContractAddress, &h1, 0)
	require.NoError(t, err)

	h2 := evmtypes.NewHead(big.NewInt(31), utils.NewHash(), utils.NewHash(), 1000, utils.NewBigI(0))
	putTurnBlockAsParent(&h2, registry)
	list2, err := orm.EligibleUpkeepsForRegistry(registry.ContractAddress, &h2, 0)
	require.NoError(t, err)

	h3 := evmtypes.NewHead(big.NewInt(42), utils.NewHash(), utils.NewHash(), 1000, utils.NewBigI(0))
	putTurnBlockAsParent(&h3, registry)
	list3, err := orm.EligibleUpkeepsForRegistry(registry.ContractAddress, &h3, 0)
	require.NoError(t, err)

	h4 := evmtypes.NewHead(big.NewInt(53), utils.NewHash(), utils.NewHash(), 1000, utils.NewBigI(0))
	putTurnBlockAsParent(&h4, registry)
	list4, err := orm.EligibleUpkeepsForRegistry(registry.ContractAddress, &h4, 0)
	require.NoError(t, err)

	// sort before compare
	sort.Slice(list1, func(i, j int) bool {
		return list1[i].UpkeepID < list1[j].UpkeepID
	})
	sort.Slice(list2, func(i, j int) bool {
		return list2[i].UpkeepID < list2[j].UpkeepID
	})
	sort.Slice(list3, func(i, j int) bool {
		return list3[i].UpkeepID < list3[j].UpkeepID
	})
	sort.Slice(list4, func(i, j int) bool {
		return list4[i].UpkeepID < list4[j].UpkeepID
	})

	assert.NotEqual(t, list1, list2, "list1 vs list2")
	assert.NotEqual(t, list1, list3, "list1 vs list3")
	assert.NotEqual(t, list1, list4, "list1 vs list4")
}

func TestKeeperDB_EligibleUpkeeps_SkipIfLastPerformedByCurrentKeeper(t *testing.T) {
	t.Parallel()
	db, config, orm := setupKeeperDB(t)
	ethKeyStore := cltest.NewKeyStore(t, db, config).Eth()

	registry, _ := cltest.MustInsertKeeperRegistry(t, db, orm, ethKeyStore, 0, 2, 20)

	for i := 0; i < 100; i++ {
		cltest.MustInsertUpkeepForRegistry(t, db, config, registry)
	}

	cltest.AssertCount(t, db, "keeper_registries", 1)
	cltest.AssertCount(t, db, "upkeep_registrations", 100)

	parentHash := utils.NewHash()
	h1 := evmtypes.NewHead(big.NewInt(21), utils.NewHash(), parentHash, 1000, utils.NewBigI(0))
	firstHeadInTurn := h1.Number - (h1.Number % int64(registry.BlockCountPerTurn))
	headParent := evmtypes.NewHead(big.NewInt(firstHeadInTurn), parentHash, utils.NewHash(), 1000, utils.NewBigI(0))
	h1.Parent = &headParent

	// if current keeper index = 0 and all upkeeps last perform was done by index = 0 then skip as it would not pass required turn taking
	upkeep := keeper.UpkeepRegistration{}
	require.NoError(t, db.Get(&upkeep, `UPDATE upkeep_registrations SET last_keeper_index = 0 RETURNING *`))
	list0, err := orm.EligibleUpkeepsForRegistry(registry.ContractAddress, &h1, 0) // none eligible
	require.NoError(t, err)
	require.Equal(t, 0, len(list0), "should be 0 as all last perform was done by current node")
}

func TestKeeperDB_EligibleUpkeeps_FirstTurn(t *testing.T) {
	t.Parallel()
	db, config, orm := setupKeeperDB(t)
	ethKeyStore := cltest.NewKeyStore(t, db, config).Eth()

	registry, _ := cltest.MustInsertKeeperRegistry(t, db, orm, ethKeyStore, 0, 2, 20)

	for i := 0; i < 100; i++ {
		cltest.MustInsertUpkeepForRegistry(t, db, config, registry)
	}

	cltest.AssertCount(t, db, "keeper_registries", 1)
	cltest.AssertCount(t, db, "upkeep_registrations", 100)

	parentHash := utils.NewHash()
	h1 := evmtypes.NewHead(big.NewInt(21), utils.NewHash(), parentHash, 1000, utils.NewBigI(0))
	firstHeadInTurn := h1.Number - (h1.Number % int64(registry.BlockCountPerTurn))
	headParent := evmtypes.NewHead(big.NewInt(firstHeadInTurn), parentHash, utils.NewHash(), 1000, utils.NewBigI(0))
	h1.Parent = &headParent

	// last keeper index is null to simulate a normal first run
	listKpr0, err := orm.EligibleUpkeepsForRegistry(registry.ContractAddress, &h1, 0) // someone eligible only kpr0 turn
	require.NoError(t, err)
	require.NotEqual(t, 0, len(listKpr0), "kpr0 should have some eligible as a normal turn")
}

func TestKeeperDB_EligibleUpkeeps_FiltersByRegistry(t *testing.T) {
	t.Parallel()
	db, config, orm := setupKeeperDB(t)
	ethKeyStore := cltest.NewKeyStore(t, db, config).Eth()

	registry1, _ := cltest.MustInsertKeeperRegistry(t, db, orm, ethKeyStore, 0, 1, 20)
	registry2, _ := cltest.MustInsertKeeperRegistry(t, db, orm, ethKeyStore, 0, 1, 20)

	cltest.MustInsertUpkeepForRegistry(t, db, config, registry1)
	cltest.MustInsertUpkeepForRegistry(t, db, config, registry2)

	cltest.AssertCount(t, db, "keeper_registries", 2)
	cltest.AssertCount(t, db, "upkeep_registrations", 2)

	head := evmtypes.NewHead(big.NewInt(20), utils.NewHash(), utils.NewHash(), 1000, utils.NewBigI(0))
	list1, err := orm.EligibleUpkeepsForRegistry(registry1.ContractAddress, &head, 0)
	require.NoError(t, err)
	list2, err := orm.EligibleUpkeepsForRegistry(registry2.ContractAddress, &head, 0)
	require.NoError(t, err)

	assert.Equal(t, 1, len(list1))
	assert.Equal(t, 1, len(list2))
}

func TestKeeperDB_NextUpkeepID(t *testing.T) {
	t.Parallel()
	db, config, orm := setupKeeperDB(t)
	ethKeyStore := cltest.NewKeyStore(t, db, config).Eth()

	registry, _ := cltest.MustInsertKeeperRegistry(t, db, orm, ethKeyStore, 0, 1, 20)

	nextID, err := orm.LowestUnsyncedID(registry.ID)
	require.NoError(t, err)
	require.Equal(t, int64(0), nextID)

	upkeep := newUpkeep(registry, 0)
	err = orm.UpsertUpkeep(&upkeep)
	require.NoError(t, err)

	nextID, err = orm.LowestUnsyncedID(registry.ID)
	require.NoError(t, err)
	require.Equal(t, int64(1), nextID)

	upkeep = newUpkeep(registry, 3)
	err = orm.UpsertUpkeep(&upkeep)
	require.NoError(t, err)

	nextID, err = orm.LowestUnsyncedID(registry.ID)
	require.NoError(t, err)
	require.Equal(t, int64(4), nextID)
}

func TestKeeperDB_SetLastRunHeightForUpkeepOnJob(t *testing.T) {
	t.Parallel()
	db, config, orm := setupKeeperDB(t)
	ethKeyStore := cltest.NewKeyStore(t, db, config).Eth()

	registry, j := cltest.MustInsertKeeperRegistry(t, db, orm, ethKeyStore, 0, 1, 20)
	upkeep := cltest.MustInsertUpkeepForRegistry(t, db, config, registry)

	require.NoError(t, orm.SetLastRunInfoForUpkeepOnJob(j.ID, upkeep.UpkeepID, 100, upkeep.Registry.FromAddress.Address()))
	assertLastRunHeight(t, db, upkeep, 100, 0)
	require.NoError(t, orm.SetLastRunInfoForUpkeepOnJob(j.ID, upkeep.UpkeepID, 0, upkeep.Registry.FromAddress.Address()))
	assertLastRunHeight(t, db, upkeep, 0, 0)
}

// turn taking algo needs the turn head hash to be in the history
func putTurnBlockAsParent(head *evmtypes.Head, registry keeper.Registry) {
	firstHeadInTurn := head.Number - (head.Number % int64(registry.BlockCountPerTurn))
	headParent := evmtypes.NewHead(big.NewInt(firstHeadInTurn), utils.NewHash(), utils.NewHash(), 1000, utils.NewBigI(0))
	head.Parent = &headParent
}
