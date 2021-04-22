// Copyright 2021 Shiwen Cheng. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package backend

import (
	"strconv"
	"sync"

	"stathat.com/c/consistent"
)

type Circle struct {
	CircleId     int // nolint:golint
	Name         string
	Backends     []*Backend
	WriteOnly    bool
	router       *consistent.Consistent
	routerCaches sync.Map
	mapToBackend map[string]*Backend
}

func NewCircle(cfg *CircleConfig, pxcfg *ProxyConfig, circleId int) (ic *Circle) { // nolint:golint
	ic = &Circle{
		CircleId:     circleId,
		Name:         cfg.Name,
		Backends:     make([]*Backend, len(cfg.Backends)),
		WriteOnly:    false,
		router:       consistent.New(),
		mapToBackend: make(map[string]*Backend),
	}
	ic.router.NumberOfReplicas = 256
	for idx, bkcfg := range cfg.Backends {
		ic.Backends[idx] = NewBackend(bkcfg, pxcfg)
		ic.addRouter(ic.Backends[idx], idx, pxcfg.HashKey)
	}
	return
}

func (ic *Circle) addRouter(be *Backend, idx int, hashKey string) {
	if hashKey == "name" {
		ic.router.Add(be.Name)
		ic.mapToBackend[be.Name] = be
	} else if hashKey == "url" {
		// compatible with version <= 2.3
		ic.router.Add(be.Url)
		ic.mapToBackend[be.Url] = be
	} else if hashKey == "exi" {
		// exi: extended index, recommended, started with 2.5+
		// no hash collision will occur before idx <= 100000, which has been tested
		str := "|" + strconv.Itoa(idx)
		ic.router.Add(str)
		ic.mapToBackend[str] = be
	} else {
		// idx: default index, compatible with version 2.4, recommended when the number of backends <= 10
		// each additional backend causes 10% hash collision from 11th backend
		str := strconv.Itoa(idx)
		ic.router.Add(str)
		ic.mapToBackend[str] = be
	}
}

func (ic *Circle) GetBackend(key string) *Backend {
	if be, ok := ic.routerCaches.Load(key); ok {
		return be.(*Backend)
	}
	value, _ := ic.router.Get(key)
	be := ic.mapToBackend[value]
	ic.routerCaches.Store(key, be)
	return be
}

func (ic *Circle) GetHealth(stats bool) interface{} {
	var wg sync.WaitGroup
	backends := make([]interface{}, len(ic.Backends))
	for i, be := range ic.Backends {
		wg.Add(1)
		go func(i int, be *Backend) {
			defer wg.Done()
			backends[i] = be.GetHealth(ic, stats)
		}(i, be)
	}
	wg.Wait()
	circle := struct {
		Id        int    `json:"id"` // nolint:golint
		Name      string `json:"name"`
		Active    bool   `json:"active"`
		WriteOnly bool   `json:"write_only"`
	}{ic.CircleId, ic.Name, ic.IsActive(), ic.WriteOnly}
	health := struct {
		Circle   interface{} `json:"circle"`
		Backends interface{} `json:"backends"`
	}{circle, backends}
	return health
}

func (ic *Circle) IsActive() bool {
	for _, be := range ic.Backends {
		if !be.IsActive() {
			return false
		}
	}
	return true
}
