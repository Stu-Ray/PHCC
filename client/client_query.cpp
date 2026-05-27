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

#include "client_query.h"
#include "mem_alloc.h"
#include "wl.h"
#include "table.h"
#include "ycsb_query.h"
#include "tpcc_query.h"
#include "tpcc_helper.h"
#include "pps_query.h"
#include "da_query.h"
#include <boost/algorithm/string.hpp>
#include <sstream>

namespace {

#if WORKLOAD == YCSB
std::string format_generated_query_log(BaseQuery * query, uint32_t server_slot, uint32_t server_node, uint32_t query_id, uint64_t tid) {
	YCSBQuery * ycsb_query = (YCSBQuery *)query;
	std::ostringstream os;
	os << "workload=YCSB node=" << g_node_id << " server_slot=" << server_slot
	   << " server_node=" << server_node << " txn_id=" << query_id << " tid=" << tid
	   << " ops=[";
	for (uint64_t i = 0; i < ycsb_query->requests.size(); i++) {
		if (i != 0) os << ";";
		os << (ycsb_query->requests[i]->acctype == RD ? "R" : "W")
		   << "(table=MAIN,key=" << ycsb_query->requests[i]->key << ")";
	}
	os << "]";
	return os.str();
}
#elif WORKLOAD == TPCC
std::string format_generated_query_log(BaseQuery * query, uint32_t server_slot, uint32_t server_node, uint32_t query_id, uint64_t tid) {
	TPCCQuery * tpcc_query = (TPCCQuery *)query;
	std::ostringstream os;
	os << "workload=TPCC node=" << g_node_id << " server_slot=" << server_slot
	   << " server_node=" << server_node << " txn_id=" << query_id << " tid=" << tid
	   << " txn_type=" << tpcc_query->txn_type
	   << " ops=[";
	switch(tpcc_query->txn_type) {
		case TPCC_PAYMENT:
			os << "W(table=WAREHOUSE,key=" << tpcc_query->w_id << ");";
			os << "W(table=DISTRICT,key=" << distKey(tpcc_query->d_id, tpcc_query->w_id) << ");";
			if (tpcc_query->by_last_name) {
				os << "R/W(table=CUSTOMER,last_name_lookup=1,w_id=" << tpcc_query->c_w_id
				   << ",d_id=" << tpcc_query->c_d_id << ",c_last=" << tpcc_query->c_last << ");";
			} else {
				os << "R/W(table=CUSTOMER,key=" << custKey(tpcc_query->c_id, tpcc_query->c_d_id, tpcc_query->c_w_id) << ");";
			}
			os << "W(table=HISTORY,key=auto)";
			break;
		case TPCC_NEW_ORDER:
			os << "R(table=WAREHOUSE,key=" << tpcc_query->w_id << ");";
			os << "R/W(table=DISTRICT,key=" << distKey(tpcc_query->d_id, tpcc_query->w_id) << ");";
			os << "R(table=CUSTOMER,key=" << custKey(tpcc_query->c_id, tpcc_query->d_id, tpcc_query->w_id) << ")";
			for (uint64_t i = 0; i < tpcc_query->items.size(); i++) {
				os << ";R(table=ITEM,key=" << tpcc_query->items[i]->ol_i_id << ")";
				os << ";R/W(table=STOCK,key=" << stockKey(tpcc_query->items[i]->ol_i_id, tpcc_query->items[i]->ol_supply_w_id) << ")";
			}
			os << ";W(table=ORDERS,key=auto);W(table=NEW_ORDER,key=auto);W(table=ORDER_LINE,key=auto)";
			break;
		case TPCC_ORDER_STATUS:
			os << "R(table=WAREHOUSE,key=" << tpcc_query->w_id << ");";
			os << "R(table=DISTRICT,key=" << distKey(tpcc_query->d_id, tpcc_query->w_id) << ");";
			if (tpcc_query->by_last_name) {
				os << "R(table=CUSTOMER,last_name_lookup=1,w_id=" << tpcc_query->w_id
				   << ",d_id=" << tpcc_query->d_id << ",c_last=" << tpcc_query->c_last << ")";
			} else {
				os << "R(table=CUSTOMER,key=" << custKey(tpcc_query->c_id, tpcc_query->d_id, tpcc_query->w_id) << ")";
			}
			os << ";R(table=ORDERS,key=auto);R(table=ORDER_LINE,key=auto)";
			break;
		case TPCC_DELIVERY:
			os << "R(table=NEW_ORDER,key=auto);R/W(table=ORDERS,key=auto);R/W(table=ORDER_LINE,key=auto);R/W(table=CUSTOMER,key=auto)";
			break;
		case TPCC_STOCK_LEVEL:
			os << "R(table=DISTRICT,key=" << distKey(tpcc_query->d_id, tpcc_query->w_id)
			   << ");R(table=ORDER_LINE,key=recent20);R(table=STOCK,key=from_order_line_items)";
			break;
		default:
			os << "UNKNOWN";
	}
	os << "]";
	return os.str();
}
#elif WORKLOAD == PPS
std::string format_generated_query_log(BaseQuery * query, uint32_t server_slot, uint32_t server_node, uint32_t query_id, uint64_t tid) {
	PPSQuery * pps_query = (PPSQuery *)query;
	std::ostringstream os;
	os << "workload=PPS node=" << g_node_id << " server_slot=" << server_slot
	   << " server_node=" << server_node << " txn_id=" << query_id << " tid=" << tid << " txn_type=" << pps_query->txn_type
	   << " part_key=" << pps_query->part_key
	   << " supplier_key=" << pps_query->supplier_key
	   << " product_key=" << pps_query->product_key;
	return os.str();
}
#endif

inline void log_generated_query(BaseQuery * query, uint32_t server_slot, uint32_t server_node, uint32_t query_id, uint64_t tid) {
#if WORKLOAD == YCSB || WORKLOAD == TPCC || WORKLOAD == PPS
	load_stmt_log_append(format_generated_query_log(query, server_slot, server_node, query_id, tid));
#else
	(void)query;
	(void)server_slot;
	(void)server_node;
	(void)query_id;
	(void)tid;
#endif
}

}

/*************************************************/
//     class Query_queue
/*************************************************/

typedef struct
{
	void* context;
	int thd_id;
}FUNC_ARGS;

void
Client_query_queue::init(Workload * h_wl) {
	_wl = h_wl;
	load_stmt_log_init();

#if SERVER_GENERATE_QUERIES
	if (ISCLIENT) return;
	size = g_thread_cnt;
#else
	  	size = g_servers_per_client;
#endif

	printf("[client_query_queue] init begin: node_id=%u, workload=%d, size(servers_per_client)=%lu, "
	       "g_max_txn_per_part=%u, g_init_parallelism=%u\n",
	       g_node_id, WORKLOAD, size, g_max_txn_per_part, g_init_parallelism);
	fflush(stdout);

#if DYNAMIC_FLAG
	std::vector<std::string> dy_write_str, dy_skew_str;
	boost::split(dy_write_str, DYNAMIC_WRITE, boost::is_any_of("|"), boost::token_compress_on);
	boost::split(dy_skew_str, DYNAMIC_SKEW, boost::is_any_of("|"), boost::token_compress_on);
	for(auto str : dy_write_str){
		dy_write.push_back(atof(str.c_str()));
	}
	for(auto str : dy_skew_str){
		dy_skew.push_back(atof(str.c_str()));
	}
	g_dy_Nbatch = std::min(dy_write.size() * dy_skew.size(), (WARMUP_TIMER + DONE_TIMER)/BILLION/SWITCH_INTERVAL);
	g_dy_batch_id = 0;
	queries.resize(g_dy_Nbatch);
	query_cnt = new uint64_t **[g_dy_Nbatch];
	for(uint32_t batch_id = 0; batch_id < g_dy_Nbatch; batch_id++){
		queries[batch_id].resize(size);
		query_cnt[batch_id] = new uint64_t*[size];
		for(uint32_t server_id = 0; server_id < size; server_id++){
			queries[batch_id][server_id].resize(g_max_txn_per_part + 4);
			query_cnt[batch_id][server_id] = new uint64_t[1];
		}
	}
#else
	query_cnt = new uint64_t * [size];
	for ( UInt32 id = 0; id < size; id ++) {
		std::vector<BaseQuery*> new_queries(g_max_txn_per_part+4,NULL);
		queries.push_back(new_queries);
		query_cnt[id] = (uint64_t*)mem_allocator.align_alloc(sizeof(uint64_t));
	}
	next_tid = 0;
#endif

#if WORKLOAD == DA
	FUNC_ARGS *arg=(FUNC_ARGS*)mem_allocator.align_alloc(sizeof(FUNC_ARGS));
	arg->context=this;
	arg->thd_id=g_init_parallelism - 1;
	pthread_t  p_thds_main;
	pthread_create(&p_thds_main, NULL, initQueriesHelper, (void*)arg );
	pthread_detach(p_thds_main);
	#else
		pthread_t * p_thds = new pthread_t[g_init_parallelism - 1];
		for (uint64_t i = 0; i < g_init_parallelism - 1; i++) {
		FUNC_ARGS *arg=(FUNC_ARGS*)mem_allocator.align_alloc(sizeof(FUNC_ARGS));
		arg->context=this;
		arg->thd_id=i;
		pthread_create(&p_thds[i], NULL, initQueriesHelper, (void*)arg );
	}
	FUNC_ARGS *arg=(FUNC_ARGS*)mem_allocator.align_alloc(sizeof(FUNC_ARGS));
	arg->context=this;
	arg->thd_id=g_init_parallelism - 1;

	initQueriesHelper(arg);

		for (uint32_t i = 0; i < g_init_parallelism - 1; i++) {
			pthread_join(p_thds[i], NULL);
		}
	#endif

	printf("[client_query_queue] init finished: node_id=%u\n", g_node_id);
	fflush(stdout);

}

void *
Client_query_queue::initQueriesHelper(void * args) {
  ((Client_query_queue*)((FUNC_ARGS*)args)->context)->initQueriesParallel(((FUNC_ARGS*)args)->thd_id);

  return NULL;
}

void
Client_query_queue::initQueriesParallel(uint64_t thd_id) {
#if WORKLOAD != DA
	UInt32 tid = ATOM_FETCH_ADD(next_tid, 1);
  uint64_t request_cnt;
	request_cnt = g_max_txn_per_part + 4;

	uint32_t final_request;
#if CC_ALG == BOCC || CC_ALG == FOCC || ONE_NODE_RECIEVE == 1
	if (tid == g_init_parallelism-1) {
		final_request = request_cnt * g_servers_per_client;
	} else {
		final_request = request_cnt * g_servers_per_client / g_init_parallelism * (tid+1);
	}
	#else
		if (tid == g_init_parallelism-1) {
			final_request = request_cnt;
		} else {
			final_request = request_cnt / g_init_parallelism * (tid+1);
		}
	#endif

	uint32_t begin_request = request_cnt / g_init_parallelism * tid;
	printf("[client_query_queue] worker begin: pthread_thd_id=%lu logical_tid=%u request_range=[%u,%u) "
	       "request_cnt=%lu servers_per_client=%u\n",
	       thd_id, tid, begin_request, final_request, request_cnt, g_servers_per_client);
	fflush(stdout);
#endif
#if WORKLOAD == YCSB
	YCSBQueryGenerator * gen = new YCSBQueryGenerator;
	gen->init();
#elif WORKLOAD == TPCC
	TPCCQueryGenerator * gen = new TPCCQueryGenerator;
	// #RAIN: init() seeds mrand for deterministic query generation when SEED != 0.
	gen->init();
#elif WORKLOAD == PPS
	PPSQueryGenerator * gen = new PPSQueryGenerator;
#elif WORKLOAD == DA
	DAQueryGenerator  * gen = new DAQueryGenerator;
#endif
#if SERVER_GENERATE_QUERIES
  #if CC_ALG == BOCC || CC_ALG == FOCC || ONE_NODE_RECIEVE == 1
	for (UInt32 query_id = request_cnt / g_init_parallelism * tid; query_id < final_request; query_id ++) {
	queries[thread_id][query_id] = gen->create_query(_wl,g_node_id);
	log_generated_query(queries[thread_id][query_id], thread_id, g_node_id, query_id, tid);
  }
  #else
  for ( UInt32 thread_id = 0; thread_id < g_thread_cnt; thread_id ++) {
	for (UInt32 query_id = request_cnt / g_init_parallelism * tid; query_id < final_request;
		 query_id++) {
	  queries[thread_id][query_id] = gen->create_query(_wl,g_node_id);
	  log_generated_query(queries[thread_id][query_id], thread_id, g_node_id, query_id, tid);
	}
  }
  #endif
#elif WORKLOAD == DA
  gen->create_query(_wl,thd_id);
#else
#if CC_ALG == BOCC || CC_ALG == FOCC || ONE_NODE_RECIEVE == 1
  for (UInt32 query_id = request_cnt / g_init_parallelism * tid; query_id < final_request; query_id ++) {
	queries[0][query_id] = gen->create_query(_wl,g_server_start_node);
	log_generated_query(queries[0][query_id], 0, g_server_start_node, query_id, tid);
  }
#else
	#if DYNAMIC_FLAG
		for(uint32_t batch_id = 0; batch_id < g_dy_Nbatch; batch_id++){
			for(uint32_t server_id = 0; server_id < g_servers_per_client; server_id++){
				for(uint32_t query_id = request_cnt / g_init_parallelism * tid; query_id < final_request; query_id++){
					queries[batch_id][server_id][query_id] = gen->create_query(_wl, server_id + g_server_start_node, batch_id);
					log_generated_query(queries[batch_id][server_id][query_id], server_id, server_id + g_server_start_node, query_id, tid);
				#if DETERMINISTIC_ABORT_MODE
					setDeterministicAbort(queries[server_id][query_id]);
				#endif
				}
			}
		}
	#else
		for ( UInt32 server_id = 0; server_id < g_servers_per_client; server_id ++) {
			for (UInt32 query_id = request_cnt / g_init_parallelism * tid; query_id < final_request;
				query_id++) {
			queries[server_id][query_id] = gen->create_query(_wl,server_id+g_server_start_node);
			log_generated_query(queries[server_id][query_id], server_id, server_id + g_server_start_node, query_id, tid);
		#if DETERMINISTIC_ABORT_MODE
			setDeterministicAbort(queries[server_id][query_id]);
		#endif
			}
		}
	#endif
#endif
#endif

#if WORKLOAD != DA
	printf("[client_query_queue] worker done: pthread_thd_id=%lu logical_tid=%u\n", thd_id, tid);
	fflush(stdout);
	load_stmt_log_flush_thread();
#endif
}

bool Client_query_queue::done() { return false; }

void Client_query_queue::setDeterministicAbort(BaseQuery *query) {
	double r = (double)(rand() % 10000) / 10000;
	query->isDeterministicAbort = (r < g_deterministic_abort_ratio);
}

BaseQuery *
Client_query_queue::get_next_query(uint64_t server_id,uint64_t thread_id) {
#if WORKLOAD == DA
  BaseQuery * query;
  query=da_gen_qry_queue.pop_data();
  //while(!da_query_queue.pop(query));
  return query;
#else
	#if DYNAMIC_FLAG
		assert(server_id < size);
		uint64_t query_id = __sync_fetch_and_add(query_cnt[g_dy_batch_id][server_id], 1);//return query_cnt[g_dy_batch_id][server_id]，then query_cnt[g_dy_batch_id][server_id]++
		if(query_id > g_max_txn_per_part) {
			__sync_bool_compare_and_swap(query_cnt[g_dy_batch_id][server_id],query_id+1,0);//if query_cnt[g_dy_batch_id][server_id]==query_id+1, then set query_cnt[g_dy_batch_id][server_id] to 0
			query_id = __sync_fetch_and_add(query_cnt[g_dy_batch_id][server_id], 1);
		}
		BaseQuery * query = queries[g_dy_batch_id][server_id][query_id];
		return query;
	#else
  assert(server_id < size);
  uint64_t query_id = __sync_fetch_and_add(query_cnt[server_id], 1);//return query_cnt[server_id]，then query_cnt[server_id]++
  if(query_id > g_max_txn_per_part) {
	__sync_bool_compare_and_swap(query_cnt[server_id],query_id+1,0);//if query_cnt[server_id]==query_id+1, then set query_cnt[server_id] to 0
	query_id = __sync_fetch_and_add(query_cnt[server_id], 1);
  }
	BaseQuery * query = queries[server_id][query_id];
	return query;
	#endif
#endif
}
