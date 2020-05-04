// Copyright 2016 PingCAP, Inc.
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

package schedule

import (
	"fmt"

	"github.com/pingcap/pd/server/cache"
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/namespace"
)

//revive:disable:unused-parameter

// Filter is an interface to filter source and target store.
type Filter interface {
	Type() string
	// Return true if the store should not be used as a source store.
	FilterSource(opt Options, store *core.StoreInfo) bool
	// Return true if the store should not be used as a target store.
	FilterTarget(opt Options, store *core.StoreInfo) bool
}

// FilterSource checks if store can pass all Filters as source store.
func FilterSource(opt Options, store *core.StoreInfo, filters []Filter) bool {
	storeAddress := store.GetAddress()
	storeID := fmt.Sprintf("%d", store.GetID())
	for _, filter := range filters {
		if filter.FilterSource(opt, store) {
			filterCounter.WithLabelValues("filter-source", storeAddress, storeID, filter.Type()).Inc()
			return true
		}
	}
	return false
}

// FilterTarget checks if store can pass all Filters as target store.
func FilterTarget(opt Options, store *core.StoreInfo, filters []Filter) bool {
	storeAddress := store.GetAddress()
	storeID := fmt.Sprintf("%d", store.GetID())
	for _, filter := range filters {
		if filter.FilterTarget(opt, store) {
			filterCounter.WithLabelValues("filter-target", storeAddress, storeID, filter.Type()).Inc()
			return true
		}
	}
	return false
}

type excludedFilter struct {
	sources map[uint64]struct{}
	targets map[uint64]struct{}
}

// NewExcludedFilter creates a Filter that filters all specified stores.
func NewExcludedFilter(sources, targets map[uint64]struct{}) Filter {
	return &excludedFilter{
		sources: sources,
		targets: targets,
	}
}

func (f *excludedFilter) Type() string {
	return "exclude-filter"
}

func (f *excludedFilter) FilterSource(opt Options, store *core.StoreInfo) bool {
	_, ok := f.sources[store.GetID()]
	return ok
}

func (f *excludedFilter) FilterTarget(opt Options, store *core.StoreInfo) bool {
	_, ok := f.targets[store.GetID()]
	return ok
}

type blockFilter struct{}

// NewBlockFilter creates a Filter that filters all stores that are blocked from balance.
func NewBlockFilter() Filter {
	return &blockFilter{}
}

func (f *blockFilter) Type() string {
	return "block-filter"
}

func (f *blockFilter) FilterSource(opt Options, store *core.StoreInfo) bool {
	return store.IsBlocked()
}

func (f *blockFilter) FilterTarget(opt Options, store *core.StoreInfo) bool {
	return store.IsBlocked()
}

type overloadFilter struct{}

// NewOverloadFilter creates a Filter that filters all stores that are overloaded from balance.
func NewOverloadFilter() Filter {
	return &overloadFilter{}
}

func (f *overloadFilter) Type() string {
	return "overload-filter"
}

func (f *overloadFilter) FilterSource(opt Options, store *core.StoreInfo) bool {
	return store.IsOverloaded()
}

func (f *overloadFilter) FilterTarget(opt Options, store *core.StoreInfo) bool {
	return store.IsOverloaded()
}

type stateFilter struct{}

// NewStateFilter creates a Filter that filters all stores that are not UP.
func NewStateFilter() Filter {
	return &stateFilter{}
}

func (f *stateFilter) Type() string {
	return "state-filter"
}

func (f *stateFilter) FilterSource(opt Options, store *core.StoreInfo) bool {
	return store.IsTombstone()
}

func (f *stateFilter) FilterTarget(opt Options, store *core.StoreInfo) bool {
	return !store.IsUp()
}

type healthFilter struct{}

// NewHealthFilter creates a Filter that filters all stores that are Busy or Down.
func NewHealthFilter() Filter {
	return &healthFilter{}
}

func (f *healthFilter) Type() string {
	return "health-filter"
}

func (f *healthFilter) filter(opt Options, store *core.StoreInfo) bool {
	if store.GetIsBusy() {
		return true
	}
	return store.DownTime() > opt.GetMaxStoreDownTime()
}

func (f *healthFilter) FilterSource(opt Options, store *core.StoreInfo) bool {
	return f.filter(opt, store)
}

func (f *healthFilter) FilterTarget(opt Options, store *core.StoreInfo) bool {
	return f.filter(opt, store)
}

type disconnectFilter struct{}

// NewDisconnectFilter creates a Filter that filters all stores that are disconnected.
func NewDisconnectFilter() Filter {
	return &disconnectFilter{}
}

func (f *disconnectFilter) Type() string {
	return "disconnect-filter"
}

func (f *disconnectFilter) FilterSource(opt Options, store *core.StoreInfo) bool {
	return store.IsDisconnected()
}

func (f *disconnectFilter) FilterTarget(opt Options, store *core.StoreInfo) bool {
	return store.IsDisconnected()
}

type pendingPeerCountFilter struct{}

// NewPendingPeerCountFilter creates a Filter that filters all stores that are
// currently handling too many pending peers.
func NewPendingPeerCountFilter() Filter {
	return &pendingPeerCountFilter{}
}

func (p *pendingPeerCountFilter) Type() string {
	return "pending-peer-filter"
}

func (p *pendingPeerCountFilter) filter(opt Options, store *core.StoreInfo) bool {
	if opt.GetMaxPendingPeerCount() == 0 {
		return false
	}
	return store.GetPendingPeerCount() > int(opt.GetMaxPendingPeerCount())
}

func (p *pendingPeerCountFilter) FilterSource(opt Options, store *core.StoreInfo) bool {
	return p.filter(opt, store)
}

func (p *pendingPeerCountFilter) FilterTarget(opt Options, store *core.StoreInfo) bool {
	return p.filter(opt, store)
}

type snapshotCountFilter struct{}

// NewSnapshotCountFilter creates a Filter that filters all stores that are
// currently handling too many snapshots.
func NewSnapshotCountFilter() Filter {
	return &snapshotCountFilter{}
}

func (f *snapshotCountFilter) Type() string {
	return "snapshot-filter"
}

func (f *snapshotCountFilter) filter(opt Options, store *core.StoreInfo) bool {
	return uint64(store.GetSendingSnapCount()) > opt.GetMaxSnapshotCount() ||
		uint64(store.GetReceivingSnapCount()) > opt.GetMaxSnapshotCount() ||
		uint64(store.GetApplyingSnapCount()) > opt.GetMaxSnapshotCount()
}

func (f *snapshotCountFilter) FilterSource(opt Options, store *core.StoreInfo) bool {
	return f.filter(opt, store)
}

func (f *snapshotCountFilter) FilterTarget(opt Options, store *core.StoreInfo) bool {
	return f.filter(opt, store)
}

type cacheFilter struct {
	cache *cache.TTLUint64
}

// NewCacheFilter creates a Filter that filters all stores that are in the cache.
func NewCacheFilter(cache *cache.TTLUint64) Filter {
	return &cacheFilter{cache: cache}
}

func (f *cacheFilter) Type() string {
	return "cache-filter"
}

func (f *cacheFilter) FilterSource(opt Options, store *core.StoreInfo) bool {
	return f.cache.Exists(store.GetID())
}

func (f *cacheFilter) FilterTarget(opt Options, store *core.StoreInfo) bool {
	return false
}

type storageThresholdFilter struct{}

// NewStorageThresholdFilter creates a Filter that filters all stores that are
// almost full.
func NewStorageThresholdFilter() Filter {
	return &storageThresholdFilter{}
}

func (f *storageThresholdFilter) Type() string {
	return "storage-threshold-filter"
}

func (f *storageThresholdFilter) FilterSource(opt Options, store *core.StoreInfo) bool {
	return false
}

func (f *storageThresholdFilter) FilterTarget(opt Options, store *core.StoreInfo) bool {
	return store.IsLowSpace(opt.GetLowSpaceRatio())
}

// distinctScoreFilter ensures that distinct score will not decrease.
type distinctScoreFilter struct {
	labels    []string
	stores    []*core.StoreInfo
	safeScore float64
}

// NewDistinctScoreFilter creates a filter that filters all stores that have
// lower distinct score than specified store.
func NewDistinctScoreFilter(labels []string, stores []*core.StoreInfo, source *core.StoreInfo) Filter {
	newStores := make([]*core.StoreInfo, 0, len(stores)-1)
	for _, s := range stores {
		if s.GetID() == source.GetID() {
			continue
		}
		newStores = append(newStores, s)
	}

	return &distinctScoreFilter{
		labels:    labels,
		stores:    newStores,
		safeScore: DistinctScore(labels, newStores, source),
	}
}

func (f *distinctScoreFilter) Type() string {
	return "distinct-filter"
}

func (f *distinctScoreFilter) FilterSource(opt Options, store *core.StoreInfo) bool {
	return false
}

func (f *distinctScoreFilter) FilterTarget(opt Options, store *core.StoreInfo) bool {
	return DistinctScore(f.labels, f.stores, store) < f.safeScore
}

type namespaceFilter struct {
	classifier namespace.Classifier
	namespace  string
}

// NewNamespaceFilter creates a Filter that filters all stores that are not
// belong to a namespace.
func NewNamespaceFilter(classifier namespace.Classifier, namespace string) Filter {
	return &namespaceFilter{
		classifier: classifier,
		namespace:  namespace,
	}
}

func (f *namespaceFilter) Type() string {
	return "namespace-filter"
}

func (f *namespaceFilter) filter(store *core.StoreInfo) bool {
	return f.classifier.GetStoreNamespace(store) != f.namespace
}

func (f *namespaceFilter) FilterSource(opt Options, store *core.StoreInfo) bool {
	return f.filter(store)
}

func (f *namespaceFilter) FilterTarget(opt Options, store *core.StoreInfo) bool {
	return f.filter(store)
}

type rejectLeaderFilter struct{}

// NewRejectLeaderFilter creates a Filter that filters stores that marked as
// rejectLeader from being the target of leader transfer.
func NewRejectLeaderFilter() Filter {
	return rejectLeaderFilter{}
}

func (f rejectLeaderFilter) Type() string {
	return "reject-leader-filter"
}

func (f rejectLeaderFilter) FilterSource(opt Options, store *core.StoreInfo) bool {
	return false
}

func (f rejectLeaderFilter) FilterTarget(opt Options, store *core.StoreInfo) bool {
	return opt.CheckLabelProperty(RejectLeader, store.GetLabels())
}

type rejectQuorumFilter struct{}

// NewRejectQuorumFilter creates a Filter that filters stores that marked as
// rejectQuorum from being the target of leader transfer.
func NewRejectQuorumFilter() Filter {
	return rejectQuorumFilter{}
}

func (f rejectQuorumFilter) Type() string {
	return "reject-quorum-filter"
}

func (f rejectQuorumFilter) FilterSource(opt Options, store *core.StoreInfo) bool {
	return false
}

func (f rejectQuorumFilter) FilterTarget(opt Options, store *core.StoreInfo) bool {
	return opt.CheckLabelProperty(RejectQuorum, store.GetLabels())
}

// StoreStateFilter is used to determine whether a store can be selected as the
// source or target of the schedule based on the store's state.
type StoreStateFilter struct {
	// Set true if the schedule involves any transfer leader operation.
	TransferLeader bool
	// Set true if the schedule involves any move region operation.
	MoveRegion bool
}

// Type returns the type of the Filter.
func (f StoreStateFilter) Type() string {
	return "store-state-filter"
}

// FilterSource returns true when the store cannot be selected as the schedule
// source.
func (f StoreStateFilter) FilterSource(opt Options, store *core.StoreInfo) bool {
	if store.IsTombstone() ||
		store.DownTime() > opt.GetMaxStoreDownTime() {
		return true
	}
	if f.TransferLeader && (store.IsDisconnected() || store.IsBlocked()) {
		return true
	}

	if f.MoveRegion && f.filterMoveRegion(opt, store) {
		return true
	}
	return false
}

// FilterTarget returns true when the store cannot be selected as the schedule
// target.
func (f StoreStateFilter) FilterTarget(opt Options, store *core.StoreInfo) bool {
	if store.IsTombstone() ||
		store.IsOffline() ||
		store.DownTime() > opt.GetMaxStoreDownTime() {
		return true
	}
	if f.TransferLeader &&
		(store.IsDisconnected() ||
			store.IsBlocked() ||
			store.GetIsBusy() ||
			opt.CheckLabelProperty(RejectLeader, store.GetLabels())) {
		return true
	}

	if f.MoveRegion {
		// only target consider the pending peers because pending more means the disk is slower.
		if opt.GetMaxPendingPeerCount() > 0 && store.GetPendingPeerCount() > int(opt.GetMaxPendingPeerCount()) {
			return true
		}

		if f.filterMoveRegion(opt, store) {
			return true
		}
	}
	return false
}

func (f StoreStateFilter) filterMoveRegion(opt Options, store *core.StoreInfo) bool {
	if store.GetIsBusy() {
		return true
	}

	if store.IsOverloaded() {
		return true
	}

	if uint64(store.GetSendingSnapCount()) > opt.GetMaxSnapshotCount() ||
		uint64(store.GetReceivingSnapCount()) > opt.GetMaxSnapshotCount() ||
		uint64(store.GetApplyingSnapCount()) > opt.GetMaxSnapshotCount() {
		return true
	}
	return false
}
