package internal

import (
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/greymass/roborovski/libraries/logger"
)

type MetadataMigrationStats struct {
	PropertiesMigrated uint64
	WALKeysMigrated    uint64
	TimeMapMigrated    uint64
	BytesRead          uint64
	BytesWritten       uint64
}

func MigrateMetadataIndexes(db *pebble.DB) (*MetadataMigrationStats, error) {
	stats := &MetadataMigrationStats{}

	logger.Printf("startup", "Phase 1/3: Migrating Properties (0x15 → 0x90)...")
	if err := migrateProperties(db, stats); err != nil {
		return nil, err
	}

	logger.Printf("startup", "Phase 2/3: Migrating WAL (0x14 → 0x91)...")
	if err := migrateWAL(db, stats); err != nil {
		return nil, err
	}

	logger.Printf("startup", "Phase 3/3: Migrating TimeMap (0x13 → 0x92)...")
	if err := migrateTimeMap(db, stats); err != nil {
		return nil, err
	}

	if err := db.Flush(); err != nil {
		return nil, err
	}

	return stats, nil
}

func migrateProperties(db *pebble.DB, stats *MetadataMigrationStats) error {
	oldKey := makeLegacyPropertiesKey()
	val, closer, err := db.Get(oldKey)
	if err == pebble.ErrNotFound {
		logger.Printf("startup", "  Properties: no legacy key found, skipping")
		return nil
	}
	if err != nil {
		return err
	}
	defer closer.Close()

	stats.BytesRead += uint64(len(val))

	newKey := makePropertiesKey()
	if err := db.Set(newKey, val, pebble.NoSync); err != nil {
		return err
	}

	stats.PropertiesMigrated = 1
	stats.BytesWritten += uint64(len(val))

	logger.Printf("startup", "  Properties: migrated 1 key")
	return nil
}

func migrateWAL(db *pebble.DB, stats *MetadataMigrationStats) error {
	start := time.Now()

	prefix := []byte{PrefixLegacyWAL}
	upperBound := []byte{PrefixLegacyWAL + 1}

	iter, err := db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: upperBound,
	})
	if err != nil {
		return err
	}
	defer iter.Close()

	batch := db.NewBatch()
	defer batch.Close()

	const batchSize = 10000
	batchCount := 0

	for iter.First(); iter.Valid(); iter.Next() {
		globalSeq, ok := parseLegacyWALKey(iter.Key())
		if !ok {
			continue
		}

		val := iter.Value()
		stats.BytesRead += uint64(len(val))

		newKey := makeWALKey(globalSeq)
		if err := batch.Set(newKey, val, pebble.NoSync); err != nil {
			return err
		}

		stats.WALKeysMigrated++
		stats.BytesWritten += uint64(len(val))
		batchCount++

		if batchCount >= batchSize {
			if err := batch.Commit(pebble.NoSync); err != nil {
				return err
			}
			batch.Reset()
			batchCount = 0
		}
	}

	if err := iter.Error(); err != nil {
		return err
	}

	if batchCount > 0 {
		if err := batch.Commit(pebble.NoSync); err != nil {
			return err
		}
	}

	logger.Printf("startup", "  WAL: migrated %d keys in %v",
		stats.WALKeysMigrated, time.Since(start).Round(time.Millisecond))
	return nil
}

func migrateTimeMap(db *pebble.DB, stats *MetadataMigrationStats) error {
	start := time.Now()

	prefix := []byte{PrefixLegacyTimeMap}
	upperBound := []byte{PrefixLegacyTimeMap + 1}

	iter, err := db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: upperBound,
	})
	if err != nil {
		return err
	}
	defer iter.Close()

	batch := db.NewBatch()
	defer batch.Close()

	const batchSize = 10000
	batchCount := 0
	lastLogTime := time.Now()

	for iter.First(); iter.Valid(); iter.Next() {
		hour, ok := parseLegacyTimeMapKey(iter.Key())
		if !ok {
			continue
		}

		val := iter.Value()
		stats.BytesRead += uint64(len(val))

		newKey := makeTimeMapKey(hour)
		if err := batch.Set(newKey, val, pebble.NoSync); err != nil {
			return err
		}

		stats.TimeMapMigrated++
		stats.BytesWritten += uint64(len(val))
		batchCount++

		if batchCount >= batchSize {
			if err := batch.Commit(pebble.NoSync); err != nil {
				return err
			}
			batch.Reset()
			batchCount = 0
		}

		if time.Since(lastLogTime) > 3*time.Second {
			elapsed := time.Since(start)
			rate := float64(stats.TimeMapMigrated) / elapsed.Seconds()
			logger.Printf("startup", "  TimeMap: %d keys (%.0f/sec)",
				stats.TimeMapMigrated, rate)
			lastLogTime = time.Now()
		}
	}

	if err := iter.Error(); err != nil {
		return err
	}

	if batchCount > 0 {
		if err := batch.Commit(pebble.NoSync); err != nil {
			return err
		}
	}

	logger.Printf("startup", "  TimeMap: migrated %d keys in %v",
		stats.TimeMapMigrated, time.Since(start).Round(time.Millisecond))
	return nil
}

func CheckMetadataMigrationNeeded(db *pebble.DB) (hasLegacy, hasNew bool) {
	hasLegacy = checkPrefixExists(db, PrefixLegacyProperties) ||
		checkPrefixExists(db, PrefixLegacyWAL) ||
		checkPrefixExists(db, PrefixLegacyTimeMap)

	hasNew = checkPrefixExists(db, PrefixProperties) ||
		checkPrefixExists(db, PrefixWAL) ||
		checkPrefixExists(db, PrefixTimeMap)

	return hasLegacy, hasNew
}

func checkPrefixExists(db *pebble.DB, prefix byte) bool {
	iter, err := db.NewIter(&pebble.IterOptions{
		LowerBound: []byte{prefix},
		UpperBound: []byte{prefix + 1},
	})
	if err != nil {
		return false
	}
	defer iter.Close()
	return iter.First()
}
