// Copyright 2018 PingCAP, Inc.
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
	"strconv"

	"github.com/pingcap/kvproto/pkg/metapb"
	log "github.com/pingcap/log"
	"github.com/pingcap/pd/server/cache"
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/schedule"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

func init() {
	schedule.RegisterScheduler("quorum", func(opController *schedule.OperatorController, args []string) (schedule.Scheduler, error) {
		return newQuorumScheduler(opController), nil
	})
}

type quorumScheduler struct {
	*baseScheduler
	selector     *schedule.BalanceSelector
	taintRegions *cache.TTLUint64
	taintStores  *cache.TTLUint64
	counter      *prometheus.CounterVec
}

// QuorumScheduler is mainly based on the store's label information for scheduling.
// Now only used for reject leader schedule, that will move the leader out of
// the store with the specific label.
func newQuorumScheduler(opController *schedule.OperatorController) schedule.Scheduler {
	taintRegions := newTaintCache()
	taintStores := newTaintCache()
	filters := []schedule.Filter{
		schedule.StoreStateFilter{MoveRegion: true},
		schedule.NewCacheFilter(taintStores),
	}
	return &quorumScheduler{
		baseScheduler: newBaseScheduler(opController),
		selector:      schedule.NewBalanceSelector(core.LeaderKind, filters),
		taintStores:   taintStores,
		taintRegions:  taintRegions,
		counter:       balanceRegionCounter,
	}
}

func (s *quorumScheduler) GetName() string {
	return "quorum-scheduler"
}

func (s *quorumScheduler) GetType() string {
	return "quorum"
}

func (s *quorumScheduler) IsScheduleAllowed(cluster schedule.Cluster) bool {
	return s.opController.OperatorCount(schedule.OpRegion) < cluster.GetRegionScheduleLimit()
}

func (s *quorumScheduler) Schedule(cluster schedule.Cluster) []*schedule.Operator {
	defer func() {
		// in case there are stupid bugs in the new scheduler, try recovering instead of letting it crash
		if r := recover(); r != nil {
			log.Error("recovered in quorum scheduler", zap.Reflect("recover", r))
		}
	}()
	schedulerCounter.WithLabelValues(s.GetName(), "schedule").Inc()
	rejectQuorumStores := rejectQuorumStores(cluster)
	if len(rejectQuorumStores) == 0 {
		schedulerCounter.WithLabelValues(s.GetName(), "skip").Inc()
		return nil
	}
	log.Debug("quorum scheduler reject quorum store list", zap.Reflect("stores", rejectQuorumStores))
	opInfluence := s.opController.GetOpInfluence(cluster)
	var hasPotentialTarget bool
	var sourceID uint64
	for id := range rejectQuorumStores {
		sourceID = id
		if region := cluster.RandFollowerRegion(id); region != nil {
			if !rejectQuorumStoreExceeded(cluster, region) {
				continue
			}
			// We don't schedule region with abnormal number of replicas.
			if len(region.GetPeers()) != cluster.GetMaxReplicas() {
				log.Debug("region has abnormal replica count", zap.String("scheduler", s.GetName()), zap.Uint64("region-id", region.GetID()))
				schedulerCounter.WithLabelValues(s.GetName(), "abnormal_replica").Inc()
				continue
			}

			// Skip hot regions.
			if cluster.IsRegionHot(region.GetID()) {
				log.Debug("region is hot", zap.String("scheduler", s.GetName()), zap.Uint64("region-id", region.GetID()))
				schedulerCounter.WithLabelValues(s.GetName(), "region_hot").Inc()
				continue
			}

			source := cluster.GetStore(id)
			if source == nil {
				continue
			}

			if !s.hasPotentialTarget(cluster, region, source, opInfluence) {
				continue
			}
			hasPotentialTarget = true

			oldPeer := region.GetStorePeer(id)
			if op := s.transferPeer(cluster, region, oldPeer, opInfluence); op != nil {
				schedulerCounter.WithLabelValues(s.GetName(), "new_operator").Inc()
				return []*schedule.Operator{op}
			}
		}
	}
	if !hasPotentialTarget {
		// If no potential target store can be found for the selected store, ignore it for a while.
		log.Debug("no operator created for selected store", zap.String("scheduler", s.GetName()), zap.Uint64("store-id", sourceID))
		s.taintStores.Put(sourceID)
	}

	schedulerCounter.WithLabelValues(s.GetName(), "no_region").Inc()
	return nil
}

// hasPotentialTarget is used to determine whether the specified sourceStore
// cannot find a matching targetStore in the long term.
// The main factor for judgment includes StoreState, DistinctScore, and
// ResourceScore, while excludes factors such as ServerBusy, too many snapshot,
// which may recover soon.
func (s *quorumScheduler) hasPotentialTarget(cluster schedule.Cluster, region *core.RegionInfo, source *core.StoreInfo, opInfluence schedule.OpInfluence) bool {
	filters := []schedule.Filter{
		schedule.NewExcludedFilter(nil, region.GetStoreIds()),
		schedule.NewDistinctScoreFilter(cluster.GetLocationLabels(), cluster.GetRegionStores(region), source),
	}

	for _, store := range cluster.GetStores() {
		if schedule.FilterTarget(cluster, store, filters) {
			continue
		}
		if !store.IsUp() || store.DownTime() > cluster.GetMaxStoreDownTime() {
			continue
		}
		return true
	}
	return false
}

// transferPeer selects the best store to create a new peer to replace the old peer.
func (s *quorumScheduler) transferPeer(cluster schedule.Cluster, region *core.RegionInfo, oldPeer *metapb.Peer, opInfluence schedule.OpInfluence) *schedule.Operator {
	// scoreGuard guarantees that the distinct score will not decrease.
	stores := cluster.GetRegionStores(region)
	source := cluster.GetStore(oldPeer.GetStoreId())
	scoreGuard := schedule.NewDistinctScoreFilter(cluster.GetLocationLabels(), stores, source)

	checker := schedule.NewReplicaChecker(cluster, nil)
	storeID, _ := checker.SelectBestReplacementStore(region, oldPeer, scoreGuard, schedule.NewRejectQuorumFilter())
	if storeID == 0 {
		schedulerCounter.WithLabelValues(s.GetName(), "no_replacement").Inc()
		return nil
	}

	target := cluster.GetStore(storeID)
	regionID := region.GetID()
	sourceID := source.GetID()
	targetID := target.GetID()
	log.Debug("", zap.Uint64("region-id", regionID), zap.Uint64("source-store", sourceID), zap.Uint64("target-store", targetID))

	newPeer, err := cluster.AllocPeer(storeID)
	if err != nil {
		schedulerCounter.WithLabelValues(s.GetName(), "no_peer").Inc()
		return nil
	}
	op, err := schedule.CreateMovePeerOperator("balance-region", cluster, region, schedule.OpBalance, oldPeer.GetStoreId(), newPeer.GetStoreId(), newPeer.GetId())
	if err != nil {
		schedulerCounter.WithLabelValues(s.GetName(), "create_operator_fail").Inc()
		return nil
	}
	sourceLabel := strconv.FormatUint(sourceID, 10)
	targetLabel := strconv.FormatUint(targetID, 10)
	s.counter.WithLabelValues("move_peer", source.GetAddress()+"-out", sourceLabel).Inc()
	s.counter.WithLabelValues("move_peer", target.GetAddress()+"-in", targetLabel).Inc()
	s.counter.WithLabelValues("direction", "from_to", sourceLabel+"-"+targetLabel).Inc()
	return op
}
