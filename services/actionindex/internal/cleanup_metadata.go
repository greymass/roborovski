package internal

import (
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/greymass/roborovski/libraries/logger"
)

type MetadataCleanupStats struct {
	PropertiesDeleted uint64
	WALDeleted        uint64
	TimeMapDeleted    uint64
	BytesDeleted      uint64
}

func CleanupLegacyMetadata(db *pebble.DB) (*MetadataCleanupStats, error) {
	stats := &MetadataCleanupStats{}

	logger.Printf("startup", "Phase 1/3: Deleting legacy Properties (0x15)...")
	if err := deleteLegacyProperties(db, stats); err != nil {
		return nil, err
	}

	logger.Printf("startup", "Phase 2/3: Deleting legacy WAL (0x14)...")
	if err := deleteLegacyWAL(db, stats); err != nil {
		return nil, err
	}

	logger.Printf("startup", "Phase 3/3: Deleting legacy TimeMap (0x13)...")
	if err := deleteLegacyTimeMap(db, stats); err != nil {
		return nil, err
	}

	if err := db.Flush(); err != nil {
		return nil, err
	}

	return stats, nil
}

func deleteLegacyProperties(db *pebble.DB, stats *MetadataCleanupStats) error {
	oldKey := makeLegacyPropertiesKey()
	val, closer, err := db.Get(oldKey)
	if err == pebble.ErrNotFound {
		logger.Printf("startup", "  Properties: no legacy key found, skipping")
		return nil
	}
	if err != nil {
		return err
	}
	stats.BytesDeleted += uint64(len(oldKey) + len(val))
	closer.Close()

	if err := db.Delete(oldKey, pebble.NoSync); err != nil {
		return err
	}

	stats.PropertiesDeleted = 1
	logger.Printf("startup", "  Properties: deleted 1 key")
	return nil
}

func deleteLegacyWAL(db *pebble.DB, stats *MetadataCleanupStats) error {
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
		key := iter.Key()
		stats.BytesDeleted += uint64(len(key) + len(iter.Value()))

		if err := batch.Delete(key, pebble.NoSync); err != nil {
			return err
		}

		stats.WALDeleted++
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

	logger.Printf("startup", "  WAL: deleted %d keys in %v",
		stats.WALDeleted, time.Since(start).Round(time.Millisecond))
	return nil
}

func deleteLegacyTimeMap(db *pebble.DB, stats *MetadataCleanupStats) error {
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
		key := iter.Key()
		stats.BytesDeleted += uint64(len(key) + len(iter.Value()))

		if err := batch.Delete(key, pebble.NoSync); err != nil {
			return err
		}

		stats.TimeMapDeleted++
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
			rate := float64(stats.TimeMapDeleted) / elapsed.Seconds()
			logger.Printf("startup", "  TimeMap: %d deleted (%.0f/sec)",
				stats.TimeMapDeleted, rate)
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

	logger.Printf("startup", "  TimeMap: deleted %d keys in %v",
		stats.TimeMapDeleted, time.Since(start).Round(time.Millisecond))
	return nil
}
