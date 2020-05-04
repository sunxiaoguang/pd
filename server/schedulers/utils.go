// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package schedulers

import (
	"time"

	"github.com/montanaflynn/stats"
	"github.com/pingcap/pd/server/cache"
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/schedule"
)

const (
	// adjustRatio is used to adjust TolerantSizeRatio according to region count.
	adjustRatio          float64 = 0.005
	minTolerantSizeRatio float64 = 1.0
)

func minUint64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

func maxUint64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

func isRegionUnhealthy(region *core.RegionInfo) bool {
	return len(region.GetDownPeers()) != 0 || len(region.GetLearners()) != 0
}

func isRejectQuorumStore(cluster schedule.Cluster, store *core.StoreInfo) bool {
	return cluster.CheckLabelProperty(schedule.RejectQuorum, store.GetLabels())
}

func numberOfRejectQuorumStores(cluster schedule.Cluster, region *core.RegionInfo) int {
	evictReplicas := 0
	for _, store := range cluster.GetRegionStores(region) {
		if isRejectQuorumStore(cluster, store) {
			evictReplicas++
		}
	}
	return evictReplicas
}

func canScheduleToRejectQuorumStore(cluster schedule.Cluster, region *core.RegionInfo) bool {
	return numberOfRejectQuorumStores(cluster, region) < cluster.GetMaxReplicas()/2
}

func rejectQuorumStoreExceeded(cluster schedule.Cluster, region *core.RegionInfo) bool {
	return numberOfRejectQuorumStores(cluster, region) >= 1+(cluster.GetMaxReplicas()/2)
}

func rejectQuorumStores(cluster schedule.Cluster) map[uint64]struct{} {
	rejectQuorumStores := make(map[uint64]struct{})
	for _, s := range cluster.GetStores() {
		if cluster.CheckLabelProperty(schedule.RejectQuorum, s.GetLabels()) {
			rejectQuorumStores[s.GetID()] = struct{}{}
		}
	}
	return rejectQuorumStores
}

func shouldBalance(cluster schedule.Cluster, source, target *core.StoreInfo, region *core.RegionInfo, kind core.ResourceKind, opInfluence schedule.OpInfluence) bool {
	// The reason we use max(regionSize, averageRegionSize) to check is:
	// 1. prevent moving small regions between stores with close scores, leading to unnecessary balance.
	// 2. prevent moving huge regions, leading to over balance.
	regionSize := region.GetApproximateSize()
	if regionSize < cluster.GetAverageRegionSize() {
		regionSize = cluster.GetAverageRegionSize()
	}

	regionSize = int64(float64(regionSize) * adjustTolerantRatio(cluster))
	sourceDelta := opInfluence.GetStoreInfluence(source.GetID()).ResourceSize(kind) - regionSize
	targetDelta := opInfluence.GetStoreInfluence(target.GetID()).ResourceSize(kind) + regionSize

	// Make sure after move, source score is still greater than target score.
	return source.ResourceScore(kind, cluster.GetHighSpaceRatio(), cluster.GetLowSpaceRatio(), sourceDelta) >
		target.ResourceScore(kind, cluster.GetHighSpaceRatio(), cluster.GetLowSpaceRatio(), targetDelta)
}

func adjustTolerantRatio(cluster schedule.Cluster) float64 {
	tolerantSizeRatio := cluster.GetTolerantSizeRatio()
	if tolerantSizeRatio == 0 {
		var maxRegionCount float64
		stores := cluster.GetStores()
		for _, store := range stores {
			regionCount := float64(cluster.GetStoreRegionCount(store.GetID()))
			if maxRegionCount < regionCount {
				maxRegionCount = regionCount
			}
		}
		tolerantSizeRatio = maxRegionCount * adjustRatio
		if tolerantSizeRatio < minTolerantSizeRatio {
			tolerantSizeRatio = minTolerantSizeRatio
		}
	}
	return tolerantSizeRatio
}

func adjustBalanceLimit(cluster schedule.Cluster, kind core.ResourceKind) uint64 {
	stores := cluster.GetStores()
	counts := make([]float64, 0, len(stores))
	for _, s := range stores {
		if s.IsUp() {
			counts = append(counts, float64(s.ResourceCount(kind)))
		}
	}
	limit, _ := stats.StandardDeviation(stats.Float64Data(counts))
	return maxUint64(1, uint64(limit))
}

const (
	taintCacheGCInterval = time.Second * 5
	taintCacheTTL        = time.Minute * 5
)

// newTaintCache creates a TTL cache to hold stores that are not able to
// schedule operators.
func newTaintCache() *cache.TTLUint64 {
	return cache.NewIDTTL(taintCacheGCInterval, taintCacheTTL)
}
