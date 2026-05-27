/*
   Copyright 2016 Massachusetts Institute of Technology

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

#include "query.h"
#include "tpcc_query.h"
#include "tpcc.h"
#include "tpcc_helper.h"
#include "mem_alloc.h"
#include "wl.h"
#include "table.h"
#include "message.h"
#include <atomic>

// #RAIN: Convenience macros that select the per-generator (deterministic) or
// global-rand (non-deterministic) path depending on whether SEED is set.
//
// Usage inside TPCCQueryGenerator member functions:
//   TPCC_RAND(max)           – like RAND(max)
//   TPCC_URand(x, y)         – like URand(x, y)
//   TPCC_NURand(A, x, y, C_) – like NURand(A, x, y) with explicit C constant
//
// In SEED==0 mode the macros fall back to the original global functions so
// behaviour is byte-for-byte identical to the unmodified code.
#if SEED != 0
#  define TPCC_RAND(max_)         RAND_r(mrand, (max_))
#  define TPCC_URand(x_, y_)      URand_r(mrand, (x_), (y_))
#  define TPCC_NURand(A_, x_, y_, C_) NURand_r(mrand, (A_), (x_), (y_), (C_))
#else
#  define TPCC_RAND(max_)         RAND(max_)
#  define TPCC_URand(x_, y_)      URand((x_), (y_))
#  define TPCC_NURand(A_, x_, y_, C_) NURand((A_), (x_), (y_))
#endif

// #RAIN: TPCCQueryGenerator::init()
// Mirrors YCSBQueryGenerator::init().  Allocates mrand and seeds it so that
// every generator instance gets an independent, deterministic RNG stream when
// SEED != 0.  Also pre-computes the three NURand C-constants from the same
// seed so that NURand output is stable across the generator's lifetime.
void TPCCQueryGenerator::init() {
    mrand = (myrand *) mem_allocator.alloc(sizeof(myrand));
#if SEED != 0
    // Each generator gets a unique seed derived from the global SEED, the
    // node id, and a monotonically increasing counter (same scheme as YCSB).
    static std::atomic<uint64_t> s_tpcc_gen_counter(0);
    uint64_t my_count = s_tpcc_gen_counter.fetch_add(1, std::memory_order_relaxed);
    uint64_t mrand_seed = (uint64_t)SEED
                        + (uint64_t)g_node_id * 1000003UL
                        + my_count * 999983UL
                        + 777777UL; // offset from YCSB counter so the two don't collide
    mrand->init(mrand_seed);
    // Pre-compute NURand C constants (one-time, per generator).
    _nur_c_255  = URand_r(mrand, 0, 255);
    _nur_c_1023 = URand_r(mrand, 0, 1023);
    _nur_c_8191 = URand_r(mrand, 0, 8191);
#else
    // SEED == 0: seed from clock (original behaviour); C constants are unused
    // because macros fall back to the global NURand() which keeps its own statics.
    mrand->init(get_sys_clock());
    _nur_c_255 = _nur_c_1023 = _nur_c_8191 = 0;
#endif
}

#if TXN_TYPE != TPCC_ALL
BaseQuery * TPCCQueryGenerator::create_query(Workload * h_wl,uint64_t home_partition_id) {
  double x = (double)(TPCC_RAND(100)) / 100.0;
	if (x < g_perc_payment)
		return gen_payment(home_partition_id);
	else
		return gen_new_order(home_partition_id);

}
#else
BaseQuery * TPCCQueryGenerator::create_query(Workload * h_wl,uint64_t home_partition_id) {
  double x = (double)(TPCC_RAND(100)) / 100.0;
  if (x <= 0.43) {
    return gen_payment(home_partition_id);
  } else if (x <= 0.47) {
    return gen_order_status(home_partition_id);
  } else if (x <= 0.51) {
    return gen_delivery(home_partition_id);
  } else if (x <= 0.55) {
    return gen_stock_level(home_partition_id);
  } else {
    return gen_new_order(home_partition_id);
  }

  // only new order and stock level
  // if (x <= 0.9375) {
  //   return gen_new_order(home_partition_id);
  // } else {
  //   return gen_stock_level(home_partition_id);
  // }
}
#endif

void TPCCQuery::init(uint64_t thd_id, Workload * h_wl) {
  items.init(g_max_items_per_txn);
  BaseQuery::init();
}

void TPCCQuery::init() {
  items.init(g_max_items_per_txn);
  BaseQuery::init();
}

void TPCCQuery::print() {

  printf(
      "TPCCQuery: %d "
      "w_id: %ld, d_id: %ld, c_id: %ld, d_w_id: %ld, c_w_id: %ld, c_d_id: %ld\n",
      (int)txn_type, w_id, d_id, c_id, d_w_id, c_w_id, c_d_id);
    if(txn_type == TPCC_NEW_ORDER) {
      printf("items: ");
      for(uint64_t size = 0; size < items.size(); size++) {
      printf("%ld, ", items[size]->ol_i_id);
      }
      printf("\n");
    }
}

std::set<uint64_t> TPCCQuery::participants(Message * msg, Workload * wl) {
  std::set<uint64_t> participant_set;
  TPCCClientQueryMessage* tpcc_msg = ((TPCCClientQueryMessage*)msg);
  uint64_t id;

  id = GET_NODE_ID(wh_to_part(tpcc_msg->w_id));
  participant_set.insert(id);

  switch(tpcc_msg->txn_type) {
    case TPCC_PAYMENT:
      id = GET_NODE_ID(wh_to_part(tpcc_msg->c_w_id));
      participant_set.insert(id);
      break;
    case TPCC_NEW_ORDER:
      for(uint64_t i = 0; i < tpcc_msg->ol_cnt; i++) {
        uint64_t req_nid = GET_NODE_ID(wh_to_part(tpcc_msg->items[i]->ol_supply_w_id));
        participant_set.insert(req_nid);
      }
      break;
    case TPCC_ORDER_STATUS:
    case TPCC_DELIVERY:
    case TPCC_STOCK_LEVEL:
      break;
    default:
      assert(false);
  }

  return participant_set;
}

uint64_t TPCCQuery::participants(bool *& pps,Workload * wl) {
  int n = 0;
  for (uint64_t i = 0; i < g_node_cnt; i++) pps[i] = false;
  uint64_t id;

  switch(txn_type) {
    case TPCC_PAYMENT:
      id = GET_NODE_ID(wh_to_part(w_id));
      if(!pps[id]) {
        pps[id] = true;
        n++;
      }
      id = GET_NODE_ID(wh_to_part(c_w_id));
      if(!pps[id]) {
        pps[id] = true;
        n++;
      }
      break;
    case TPCC_NEW_ORDER:
      id = GET_NODE_ID(wh_to_part(w_id));
      if(!pps[id]) {
        pps[id] = true;
        n++;
      }
      /*
      id = GET_NODE_ID(wh_to_part(c_w_id));
      if(!pps[id]) {
        pps[id] = true;
        n++;
      }
      id = GET_NODE_ID(wh_to_part(d_w_id));
      if(!pps[id]) {
        pps[id] = true;
        n++;
      }
      */
      for(uint64_t i = 0; i < ol_cnt; i++) {
        uint64_t req_nid = GET_NODE_ID(wh_to_part(items[i]->ol_supply_w_id));
        if(!pps[req_nid]) {
          pps[req_nid] = true;
          n++;
        }
      }
      break;
    default:
      assert(false);
  }

  return n;
}

bool TPCCQuery::readonly() { return false; }

BaseQuery * TPCCQueryGenerator::gen_payment(uint64_t home_partition) {
  TPCCQuery * query = new TPCCQuery;
	set<uint64_t> partitions_accessed;

	query->txn_type = TPCC_PAYMENT;
  uint64_t home_warehouse;
	if (FIRST_PART_LOCAL) {
    while (wh_to_part(home_warehouse = TPCC_URand(1, g_num_wh)) != home_partition) {
  }
  } else
		home_warehouse = TPCC_URand(1, g_num_wh);
  query->w_id =  home_warehouse;
	query->d_w_id = home_warehouse;

  partitions_accessed.insert(wh_to_part(query->w_id));

	query->d_id = TPCC_URand(1, g_dist_per_wh);
	query->h_amount = TPCC_URand(1, 5000);
  query->rbk = false;
	double x = (double)(TPCC_RAND(10000)) / 10000;
	int y = TPCC_URand(1, 100);

	// if(x > g_mpr) {
#ifdef NO_REMOTE
  if(x >= 0) {
#else
	if(x > g_mpr) {
#endif
		// home warehouse
		query->c_d_id = query->d_id;
		query->c_w_id = query->w_id;
	} else {
		// remote warehouse
		query->c_d_id = TPCC_URand(1, g_dist_per_wh);
		if(g_num_wh > 1) {
      //we need to assure that txn access remote partition at least once
      while(wh_to_part(query->c_w_id = TPCC_URand(1, g_num_wh)) == home_partition){
      }
      partitions_accessed.insert(wh_to_part(query->c_w_id));
		} else{
      query->c_w_id = query->w_id;
    }
	}
	if(y <= 60) {
		// by last name
		query->by_last_name = true;
		Lastname(TPCC_NURand(255, 0, 999, _nur_c_255), query->c_last);
	} else {
		// by cust id
		query->by_last_name = false;
		query->c_id = TPCC_NURand(1023, 1, g_cust_per_dist, _nur_c_1023);
	}

  query->partitions.init(partitions_accessed.size());
  for(auto it = partitions_accessed.begin(); it != partitions_accessed.end(); ++it) {
    query->partitions.add(*it);
  }
  return query;
}

BaseQuery * TPCCQueryGenerator::gen_new_order(uint64_t home_partition) {
  TPCCQuery * query = new TPCCQuery;
	set<uint64_t> partitions_accessed;

	query->txn_type = TPCC_NEW_ORDER;
  query->items.init(g_max_items_per_txn);
	if (FIRST_PART_LOCAL) {
    while (wh_to_part(query->w_id = TPCC_URand(1, g_num_wh)) != home_partition) {
    }
  } else{
    query->w_id = TPCC_URand(1, g_num_wh);
  }

	query->d_id = TPCC_URand(1, g_dist_per_wh);
	query->c_id = TPCC_NURand(1023, 1, g_cust_per_dist, _nur_c_1023);
  // TODO TPCC rollback
	//rbk = URand(1, 100) == 1 ? true : false;
	query->rbk = false;
	query->ol_cnt = TPCC_URand(5, g_max_items_per_txn);
	query->o_entry_d = 2013;

  partitions_accessed.insert(wh_to_part(query->w_id));

  double r_mpr = (double)(TPCC_RAND(10000)) / 10000;
  uint64_t part_limit;
#ifdef NO_REMOTE
  if(r_mpr < 0)
#else
	if(r_mpr < g_mpr_neworder)
#endif
    part_limit = g_part_per_txn;
  else
    part_limit = 1;

  std::set<uint64_t> ol_i_ids;
  while(query->items.size() < query->ol_cnt) {
      Item_no * item = new Item_no;

    while (ol_i_ids.count(item->ol_i_id = TPCC_NURand(8191, 1, g_max_items, _nur_c_8191)) > 0) {
    }
    ol_i_ids.insert(item->ol_i_id);
    item->ol_quantity = TPCC_URand(1, 10);
		if (r_mpr > g_mpr_neworder || g_node_cnt == 1) {
			// home warehouse
			item->ol_supply_w_id = query->w_id;
    } else {
      if(partitions_accessed.size() < part_limit) {
        if(query->items.size() == 0){
          //we need to assure that txn access remote partition at least once
          while(wh_to_part(item->ol_supply_w_id = TPCC_URand(1, g_num_wh)) == home_partition){
          }
        }else{
          item->ol_supply_w_id = TPCC_URand(1, g_num_wh);
        }
        partitions_accessed.insert(wh_to_part(item->ol_supply_w_id));
      } else {
        // select warehouse from among those already selected
        while (partitions_accessed.count(wh_to_part(item->ol_supply_w_id = TPCC_URand(1, g_num_wh))) == 0) {
        }
      }
    }

    query->items.add(item);
  }

  query->partitions.init(partitions_accessed.size());
  for(auto it = partitions_accessed.begin(); it != partitions_accessed.end(); ++it) {
    query->partitions.add(*it);
  }
  return query;

}

BaseQuery * TPCCQueryGenerator::gen_order_status(uint64_t home_partition) {
  TPCCQuery * query = new TPCCQuery;
  set<uint64_t> partitions_accessed;

  query->txn_type = TPCC_ORDER_STATUS;

  while (wh_to_part(query->w_id = TPCC_URand(1, g_num_wh)) != home_partition) {}

  partitions_accessed.insert(wh_to_part(query->w_id));

  query->d_id = TPCC_URand(1, g_dist_per_wh);

  int y = TPCC_URand(1, 100);
  if(y <= 60) {
		// by last name
		query->by_last_name = true;
		Lastname(TPCC_NURand(255, 0, 999, _nur_c_255), query->c_last);
	} else {
		// by cust id
		query->by_last_name = false;
		query->c_id = TPCC_NURand(1023, 1, g_cust_per_dist, _nur_c_1023);
	}

  query->rbk = false;

  query->partitions.init(partitions_accessed.size());
  for(auto it = partitions_accessed.begin(); it != partitions_accessed.end(); ++it) {
    query->partitions.add(*it);
  }
  return query;
}

BaseQuery * TPCCQueryGenerator::gen_delivery(uint64_t home_partition) {
  TPCCQuery * query = new TPCCQuery;
  set<uint64_t> partitions_accessed;

  query->txn_type = TPCC_DELIVERY;

  while (wh_to_part(query->w_id = TPCC_URand(1, g_num_wh)) != home_partition) {}

  partitions_accessed.insert(wh_to_part(query->w_id));

  query->d_id = TPCC_URand(1, g_dist_per_wh);
  query->o_carrier_id = TPCC_URand(1, 10);
  query->ol_delivery_d = 2013;

  query->rbk = false;

  query->partitions.init(partitions_accessed.size());
  for(auto it = partitions_accessed.begin(); it != partitions_accessed.end(); ++it) {
    query->partitions.add(*it);
  }
  return query;
}

BaseQuery * TPCCQueryGenerator::gen_stock_level(uint64_t home_partition) {
  TPCCQuery * query = new TPCCQuery;
  set<uint64_t> partitions_accessed;

  query->txn_type = TPCC_STOCK_LEVEL;

  while (wh_to_part(query->w_id = TPCC_URand(1, g_num_wh)) != home_partition) {}

  partitions_accessed.insert(wh_to_part(query->w_id));

  query->d_id = TPCC_URand(1, g_dist_per_wh);
  query->threshold = TPCC_URand(10, 20);

  query->rbk = false;

  query->partitions.init(partitions_accessed.size());
  for(auto it = partitions_accessed.begin(); it != partitions_accessed.end(); ++it) {
    query->partitions.add(*it);
  }
  return query;
}

uint64_t TPCCQuery::get_participants(Workload * wl) {
   uint64_t participant_cnt = 0;
   uint64_t active_cnt = 0;
  assert(participant_nodes.size()==0);
  assert(active_nodes.size()==0);
  for(uint64_t i = 0; i < g_node_cnt; i++) {
      participant_nodes.add(0);
      active_nodes.add(0);
  }
  assert(participant_nodes.size()==g_node_cnt);
  assert(active_nodes.size()==g_node_cnt);

  uint64_t home_wh_node;
  home_wh_node = GET_NODE_ID(wh_to_part(w_id));
  participant_nodes.set(home_wh_node,1);
  active_nodes.set(home_wh_node,1);
  participant_cnt++;
  active_cnt++;
  if(txn_type == TPCC_PAYMENT) {
      uint64_t req_nid = GET_NODE_ID(wh_to_part(c_w_id));
      if(participant_nodes[req_nid] == 0) {
        participant_cnt++;
        participant_nodes.set(req_nid,1);
        active_cnt++;
        active_nodes.set(req_nid,1);
      }

  } else if (txn_type == TPCC_NEW_ORDER) {
    for(uint64_t i = 0; i < ol_cnt; i++) {
      uint64_t req_nid = GET_NODE_ID(wh_to_part(items[i]->ol_supply_w_id));
      if(participant_nodes[req_nid] == 0) {
        participant_cnt++;
        participant_nodes.set(req_nid,1);
        active_cnt++;
        active_nodes.set(req_nid,1);
      }
    }
  }
  return participant_cnt;
}

#if CC_ALG == HDCC || CC_ALG == SNAPPER
void TPCCQuery::reset(int algo){
  BaseQuery::clear();
  if(algo != CALVIN){
    release_items();
  }
  items.clear();
}
#else
void TPCCQuery::reset() {
  BaseQuery::clear();
#if CC_ALG != CALVIN
  release_items();
#endif
  items.clear();
}
#endif

void TPCCQuery::release() {
  BaseQuery::release();
  DEBUG_M("TPCCQuery::release() free\n");
#if CC_ALG != CALVIN
  release_items();
#endif
  items.release();
}

void TPCCQuery::release_items() {
  // A bit of a hack to ensure that original requests in client query queue aren't freed
  if (SERVER_GENERATE_QUERIES) return;
  for(uint64_t i = 0; i < items.size(); i++) {
    DEBUG_M("TPCCQuery::release() Item_no free\n");
    mem_allocator.free(items[i],sizeof(Item_no));
  }

}
