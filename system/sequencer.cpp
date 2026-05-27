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

#include "global.h"
#include "sequencer.h"
#include "ycsb_query.h"
#include "da_query.h"
#include "tpcc_query.h"
#include "pps_query.h"
#include "mem_alloc.h"
#include "transport.h"
#include "wl.h"
#include "helper.h"
#include "msg_queue.h"
#include "msg_thread.h"
#include "work_queue.h"
#include "message.h"
#include "stats.h"
#include <boost/lockfree/queue.hpp>
#if CC_ALG == HDCC || CC_ALG == SNAPPER
#include "cc_selector.h"
#endif

#include "table.h"
#include "tpcc_helper.h"
#include "tpcc.h"
#include "active_ws_table.h"
#include "workset_predictor.h"

/* ------------------------- #RAIN predict ------------------------- */

#if CC_ALG == HDCC

/*
 * TPCC key 编码：与 txn.cpp / worker_thread.cpp 完全一致。
 * 格式：(table_id << 56) | (primary_key & 0x00ffffffffffffff)
 * 这样不同表的 primary key 不会产生数值空间重叠，
 * 且在执行期（txn_ws_key）和路由期（seq_ws_key）使用相同的键空间。
 *
 * TPCC table_id 由 schema 文件顺序决定，在 Sequencer::init 中
 * 从 Workload 对象读取并缓存到 s_tpcc_table_ids。
 */
#if WORKLOAD == TPCC
struct TPCCSeqTableIds {
    uint64_t warehouse;
    uint64_t district;
    uint64_t customer_by_id;
    uint64_t customer_by_name;
    uint64_t item;
    uint64_t stock;
    bool ready;
    TPCCSeqTableIds() : warehouse(0), district(0), customer_by_id(0),
                        customer_by_name(0), item(0), stock(0), ready(false) {}
};
static TPCCSeqTableIds s_tpcc_table_ids;

// 与 txn.cpp::txn_ws_key 完全等价的 Sequencer 侧编码
static inline uint64_t seq_ws_key(uint64_t table_id, uint64_t primary_key) {
    return (table_id << 56) | (primary_key & 0x00ffffffffffffffUL);
}
#endif // WORKLOAD == TPCC

static inline void add_ws_item(std::vector<WsItem> &items, uint64_t key, WsMode mode, WsSrc src, double conf) {
    WsItem item;
    item.key = key;
    item.mode = mode;
    item.src = src;
    item.conf = conf;
    items.push_back(item);
}

/*
 * 从客户端消息中提取"已知工作集"。
 *
 * 注意：
 * - 方案一是在事务还没真正执行时做预测。
 * - 因此这里的 WS_ACTUAL 不是"已经执行过"，而是"输入中确定存在的访问项，置信度=1"。
 * - 预测器输出的项才标记为 WS_PREDICTED。
 * - TPCC key 编码统一使用 (table_id<<56)|primary_key，与 txn.cpp::txn_ws_key 一致。
 */
static inline void extract_workset_items(Message *msg, std::vector<WsItem> &items) {
    items.clear();

#if WORKLOAD == YCSB
    YCSBClientQueryMessage *ycsb_msg = (YCSBClientQueryMessage *)msg;
    items.reserve(ycsb_msg->requests.size());

    for (uint64_t i = 0; i < ycsb_msg->requests.size(); i++) {
        ycsb_request *req = ycsb_msg->requests[i];
        WsMode mode = (req->acctype == WR) ? WS_WR : WS_RD;
        add_ws_item(items, req->key, mode, WS_ACTUAL, 1.0);
    }

#elif WORKLOAD == TPCC
    if (!s_tpcc_table_ids.ready) return; // init 未完成，跳过

    TPCCClientQueryMessage *tpcc_msg = (TPCCClientQueryMessage *)msg;
    items.reserve(8 + tpcc_msg->items.size() * 2);

    switch (tpcc_msg->txn_type) {
        case TPCC_PAYMENT: {
            // WAREHOUSE: payment 会更新 W_YTD
            add_ws_item(items,
                        seq_ws_key(s_tpcc_table_ids.warehouse, tpcc_msg->w_id),
                        WS_WR, WS_ACTUAL, 1.0);

            // DISTRICT: payment 会更新 D_YTD
            add_ws_item(items,
                        seq_ws_key(s_tpcc_table_ids.district,
                                   distKey(tpcc_msg->d_id, tpcc_msg->d_w_id)),
                        WS_WR, WS_ACTUAL, 1.0);

            // CUSTOMER: payment 会更新 customer balance / ytd / payment count
            if (tpcc_msg->by_last_name) {
                add_ws_item(items,
                            seq_ws_key(s_tpcc_table_ids.customer_by_name,
                                       custNPKey(tpcc_msg->c_last,
                                                 tpcc_msg->c_d_id,
                                                 tpcc_msg->c_w_id)),
                            WS_WR, WS_ACTUAL, 1.0);
            } else {
                add_ws_item(items,
                            seq_ws_key(s_tpcc_table_ids.customer_by_id,
                                       custKey(tpcc_msg->c_id,
                                               tpcc_msg->c_d_id,
                                               tpcc_msg->c_w_id)),
                            WS_WR, WS_ACTUAL, 1.0);
            }
            break;
        }

        case TPCC_NEW_ORDER: {
            // WAREHOUSE: new-order 通常读取税率
            add_ws_item(items,
                        seq_ws_key(s_tpcc_table_ids.warehouse, tpcc_msg->w_id),
                        WS_RD, WS_ACTUAL, 1.0);

            // CUSTOMER: new-order 通常读取折扣/信用/姓氏
            add_ws_item(items,
                        seq_ws_key(s_tpcc_table_ids.customer_by_id,
                                   custKey(tpcc_msg->c_id,
                                           tpcc_msg->d_id,
                                           tpcc_msg->w_id)),
                        WS_RD, WS_ACTUAL, 1.0);

            // DISTRICT: new-order 更新 next_o_id
            add_ws_item(items,
                        seq_ws_key(s_tpcc_table_ids.district,
                                   distKey(tpcc_msg->d_id, tpcc_msg->w_id)),
                        WS_WR, WS_ACTUAL, 1.0);

            // ITEM + STOCK
            for (uint64_t i = 0; i < tpcc_msg->items.size(); i++) {
                Item_no *item = tpcc_msg->items[i];

                add_ws_item(items,
                            seq_ws_key(s_tpcc_table_ids.item, item->ol_i_id),
                            WS_RD, WS_ACTUAL, 1.0);

                add_ws_item(items,
                            seq_ws_key(s_tpcc_table_ids.stock,
                                       stockKey(item->ol_i_id,
                                                item->ol_supply_w_id)),
                            WS_WR, WS_ACTUAL, 1.0);
            }
            break;
        }

        default:
            // 其他 TPCC 类型暂时给一个粗粒度 key，保证不会空集合。
            add_ws_item(items,
                        seq_ws_key(s_tpcc_table_ids.warehouse, tpcc_msg->w_id),
                        WS_UNK, WS_ACTUAL, 1.0);
            break;
    }
#endif
}

/*
 * 从 items 中取前 limit 个 key 作为 predictor 上下文。
 * limit==0 表示取全部。
 */
static inline void items_to_keys(const std::vector<WsItem> &items,
                                 std::vector<uint64_t> &keys,
                                 uint32_t limit) {
    keys.clear();

    uint32_t real_limit = limit == 0 ? items.size() : limit;
    for (uint64_t i = 0; i < items.size() && i < real_limit; i++) {
        keys.push_back(items[i].key);
    }
}

/*
 * predictor 输出转 ActiveWsTable 可识别的 WsItem。
 */
static inline void preds_to_ws_items(
        const std::vector<WorksetPredictor::PredItem> &preds,
        std::vector<WsItem> &items) {
    items.clear();
    items.reserve(preds.size());

    for (auto &pred : preds) {
        add_ws_item(items, pred.item, WS_UNK, WS_PREDICTED, pred.prob);
    }
}

static inline bool has_high_predict_risk(
        const std::vector<WsConflictHit> &hits) {
    for (auto &hit : hits) {
        if (hit.risk >= g_predict_risk_threshold) {
            return true;
        }
    }
    return false;
}

/*
 * 方案一：事务进入 Sequencer 时执行。
 *
 * 流程：
 * 1. 从客户端消息提取已知工作集；
 * 2. 取前 g_k_value 个 key 作为上下文；
 * 3. 调 WorksetPredictor::predict；
 * 4. 将"已知前缀 + 预测项"注册到 ActiveWsTable；
 * 5. 检测与其他活跃事务的冲突风险；
 * 6. 若风险高，返回 true，Sequencer 将该事务分流到 Calvin。
 */
static inline bool should_route_to_calvin_by_prediction(Message *msg) {
    if (g_predict_mode != 1) {
        return false;
    }

    std::vector<WsItem> known_items;
    extract_workset_items(msg, known_items);

    if (known_items.empty()) {
        return false;
    }

    std::vector<uint64_t> prefix_keys;
    items_to_keys(known_items, prefix_keys, g_k_value);

    std::vector<WorksetPredictor::PredItem> preds =
        workset_predictor.predict(prefix_keys, g_predict_topk);

    std::vector<WsItem> pred_items;
    preds_to_ws_items(preds, pred_items);

    std::vector<WsItem> candidate;

    // 只注册前 k 个"当前已知项"，避免在方案一中过早泄漏完整未来工作集。
    uint32_t prefix_cnt = g_k_value == 0 ? known_items.size() : g_k_value;
    for (uint64_t i = 0; i < known_items.size() && i < prefix_cnt; i++) {
        candidate.push_back(known_items[i]);
    }

    candidate.insert(candidate.end(), pred_items.begin(), pred_items.end());

    active_ws_table.register_items(msg->txn_id, candidate);

    std::vector<WsConflictHit> hits =
        active_ws_table.detect_conflicts(msg->txn_id, candidate);

    return has_high_predict_risk(hits);
}

/* ── 方案三（PREDICT_MODE == 3）──────────────────────────────────────────────
 *
 * 设计目标：
 *   方案一只使用前 k 个已知项作为 predictor 上下文，对于 TPCC PAYMENT / NEW_ORDER 而言，完整工作集完全由 query 参数静态决定，根本无需预测补全。
 *   方案三消除对 WorksetPredictor 的依赖，改为直接用完整静态工作集做精确冲突检测，从而消除"前缀截断 + 预测误差"带来的路由误判。
 *
 * 适用场景（当前实验）：
 *   WORKLOAD == TPCC，TXN_TYPE != TPCC_ALL（即仅 PAYMENT / NEW_ORDER）。
 *   两种事务类型的工作集均完全由 query 参数静态确定：
 *     - PAYMENT：WAREHOUSE(WR) + DISTRICT(WR) + CUSTOMER(WR)，3 个精确 key
 *     - NEW_ORDER：WAREHOUSE(RD) + CUSTOMER(RD) + DISTRICT(WR) + N×(ITEM+STOCK)，全部静态可知
 *   因此 extract_workset_items() 提取的结果即为 100% 完整工作集，extract_full_workset_items() 直接复用，无需任何 dry-run 补充。
 *
 * 与方案一的区别：
 *   - 方案一：只取前 k 项 + predictor 预测补全（有误差、依赖训练数据）
 *   - 方案三：取全部项（100% 精确）+ 直接冲突检测（无 predictor 依赖）
 *
 * 与方案二的区别：
 *   - 方案二：SILO 执行到第 k 步后触发，有 abort 代价
 *   - 方案三：Sequencer 入口（执行前）触发，abort 代价为零
 *
 * 注：若未来实验扩展到 TPCC_ALL（含 DELIVERY/ORDER_STATUS），
 *     需引入 dry-run 预执行相关的补充逻辑。
 * ────────────────────────────────────────────────────────────────────────────*/

/*
 * extract_full_workset_items：
 *   方案三专用。提取事务完整工作集，不限制前 k 项，不调用 predictor。
 *   对于 TPCC PAYMENT / NEW_ORDER：extract_workset_items() 已返回完整工作集，直接透传，无需任何补充。
 */
static inline void extract_full_workset_items(Message *msg,
                                               std::vector<WsItem> &items) {
    // PAYMENT / NEW_ORDER 的工作集完全由 query 参数静态决定，
    // extract_workset_items() 已经精确、完整，无需 dry-run 补充。
    extract_workset_items(msg, items);
}

/*
 * should_route_to_calvin_by_full_workset（方案三入口）：
 *
 * 流程：
 *   1. 提取完整工作集（PAYMENT/NEW_ORDER 均 100% 精确）；
 *   2. 注册到 ActiveWsTable；
 *   3. 检测与其他活跃事务的冲突风险（不调用 WorksetPredictor）；
 *   4. 风险高 → 返回 true，Sequencer 路由到 Calvin。
 */
static inline bool should_route_to_calvin_by_full_workset(Message *msg) {
    if (g_predict_mode != 3) {
        return false;
    }

    std::vector<WsItem> full_items;
    extract_full_workset_items(msg, full_items);

    if (full_items.empty()) {
        return false;
    }

    // 注册完整工作集（不限 k 个前缀）
    active_ws_table.register_items(msg->txn_id, full_items);

    std::vector<WsConflictHit> hits =
        active_ws_table.detect_conflicts(msg->txn_id, full_items);

    return has_high_predict_risk(hits);
}

#endif

/* ------------------------- #RAIN end ------------------------- */

void Sequencer::init(Workload * wl) {
	next_txn_id = 0;
	rsp_cnt = g_node_cnt + g_client_node_cnt;
	_wl = wl;
	last_time_batch = 0;
	wl_head = NULL;
	wl_tail = NULL;
	fill_queue = new boost::lockfree::queue<Message*, boost::lockfree::capacity<65526> > [g_node_cnt];
#if CC_ALG == HDCC || CC_ALG == SNAPPER
	last_epoch_max_id = 0;
	blocked = false;
	validationCount = 0;
#endif

#if CC_ALG == HDCC && WORKLOAD == TPCC
	/*
	 * 缓存 TPCC 各表的 table_id，用于 extract_workset_items 中的
	 * seq_ws_key(table_id, primary_key) 编码——与 txn.cpp::txn_ws_key 统一。
	 */
	{
		TPCCWorkload * tpcc_wl = (TPCCWorkload *)wl;
		s_tpcc_table_ids.warehouse        = tpcc_wl->t_warehouse->get_table_id();
		s_tpcc_table_ids.district         = tpcc_wl->t_district->get_table_id();
		s_tpcc_table_ids.customer_by_id   = tpcc_wl->t_customer->get_table_id();
		// customer_by_name 共享同一张实体表，table_id 相同
		s_tpcc_table_ids.customer_by_name = tpcc_wl->t_customer->get_table_id();
		s_tpcc_table_ids.item             = tpcc_wl->t_item->get_table_id();
		s_tpcc_table_ids.stock            = tpcc_wl->t_stock->get_table_id();
		s_tpcc_table_ids.ready            = true;
	}
#endif
}

// Assumes 1 thread does sequencer work
void Sequencer::process_ack(Message * msg, uint64_t thd_id) {
	qlite_ll * en = wl_head;
	while(en != NULL && en->epoch != msg->get_batch_id()) {
		en = en->next;
	}
	assert(en);
	qlite * wait_list = en->list;
	assert(wait_list != NULL);
	assert(en->txns_left > 0);

#if CC_ALG == HDCC || CC_ALG == SNAPPER
	uint64_t id = (msg->get_txn_id() - en->start_txn_id) / g_node_cnt;
#else
	uint64_t id = msg->get_txn_id() / g_node_cnt;
#endif

	uint64_t prof_stat = get_sys_clock();
	assert(wait_list[id].server_ack_cnt > 0);

	// Decrement the number of acks needed for this txn
	uint32_t query_acks_left = ATOM_SUB_FETCH(wait_list[id].server_ack_cnt, 1);

	if (wait_list[id].skew_startts == 0) {
			wait_list[id].skew_startts = get_sys_clock();
	}

	if (query_acks_left == 0) {
			en->txns_left--;
			ATOM_FETCH_ADD(total_txns_finished,1);
			INC_STATS(thd_id,seq_txn_cnt,1);
			// free msg, queries
#if WORKLOAD == YCSB
			YCSBClientQueryMessage* cl_msg = (YCSBClientQueryMessage*)wait_list[id].msg;
#if CC_ALG == HDCC || CC_ALG == SNAPPER
			if (msg->algo == CALVIN) {
#endif
			for(uint64_t i = 0; i < cl_msg->requests.size(); i++) {
					DEBUG_M("Sequencer::process_ack() ycsb_request free\n");
					mem_allocator.free(cl_msg->requests[i],sizeof(ycsb_request));
			}
#if CC_ALG == HDCC || CC_ALG == SNAPPER
			}
#endif
#elif WORKLOAD == TPCC
			TPCCClientQueryMessage* cl_msg = (TPCCClientQueryMessage*)wait_list[id].msg;
#if CC_ALG == HDCC || CC_ALG == SNAPPER
	if(msg->algo == CALVIN){
		if(cl_msg->txn_type == TPCC_NEW_ORDER) {
			for(uint64_t i = 0; i < cl_msg->items.size(); i++) {
					DEBUG_M("Sequencer::process_ack() items free\n");
					mem_allocator.free(cl_msg->items[i],sizeof(Item_no));
			}
		}
	}
#elif CC_ALG==CALVIN
			if(cl_msg->txn_type == TPCC_NEW_ORDER) {
					for(uint64_t i = 0; i < cl_msg->items.size(); i++) {
							DEBUG_M("Sequencer::process_ack() items free\n");
							mem_allocator.free(cl_msg->items[i],sizeof(Item_no));
					}
			}
#endif
#elif WORKLOAD == PPS
			PPSClientQueryMessage* cl_msg = (PPSClientQueryMessage*)wait_list[id].msg;

#elif WORKLOAD == DA
			DAClientQueryMessage* cl_msg = (DAClientQueryMessage*)wait_list[id].msg;
#endif
#if WORKLOAD == PPS
		if (WORKLOAD == PPS && CC_ALG == CALVIN &&
				((cl_msg->txn_type == PPS_GETPARTBYSUPPLIER) ||
				 (cl_msg->txn_type == PPS_GETPARTBYPRODUCT) || (cl_msg->txn_type == PPS_ORDERPRODUCT)) &&
				(cl_msg->recon || ((AckMessage *)msg)->rc == Abort)) {
					int abort_cnt = wait_list[id].abort_cnt;
					if (cl_msg->recon) {
							// Copy over part keys
							cl_msg->part_keys.copy( ((AckMessage*)msg)->part_keys);
							DEBUG("Finished RECON (%ld,%ld)\n",msg->get_txn_id(),msg->get_batch_id());
			} else {
							uint64_t timespan = get_sys_clock() - wait_list[id].seq_startts;
							if (warmup_done) {
								INC_STATS_ARR(0,start_abort_commit_latency, timespan);
							}
							cl_msg->part_keys.clear();
							DEBUG("Aborted (%ld,%ld)\n",msg->get_txn_id(),msg->get_batch_id());
							INC_STATS(0,total_txn_abort_cnt,1);
							abort_cnt++;
					}

					cl_msg->return_node_id = wait_list[id].client_id;
					wait_list[id].total_batch_time += en->batch_send_time - wait_list[id].seq_startts;
					// restart
			process_txn(cl_msg, thd_id, wait_list[id].seq_first_startts, wait_list[id].seq_startts,
									wait_list[id].total_batch_time, abort_cnt);
		} else {
#endif
					uint64_t curr_clock = get_sys_clock();
					uint64_t timespan = curr_clock - wait_list[id].seq_first_startts;
					uint64_t timespan2 = curr_clock - wait_list[id].seq_startts;
					uint64_t skew_timespan = get_sys_clock() - wait_list[id].skew_startts;
					wait_list[id].total_batch_time += en->batch_send_time - wait_list[id].seq_startts;
					if (warmup_done) {
						INC_STATS_ARR(0,first_start_commit_latency, timespan);
						INC_STATS_ARR(0,last_start_commit_latency, timespan2);
						INC_STATS_ARR(0,start_abort_commit_latency, timespan2);
					}
					if (wait_list[id].abort_cnt > 0) {
							INC_STATS(0,unique_txn_abort_cnt,1);
					}

		INC_STATS(0,lat_l_loc_msg_queue_time,wait_list[id].total_batch_time);
		INC_STATS(0,lat_l_loc_process_time,skew_timespan);

		INC_STATS(0,lat_short_work_queue_time,msg->lat_work_queue_time);
		INC_STATS(0,lat_short_msg_queue_time,msg->lat_msg_queue_time);
		INC_STATS(0,lat_short_cc_block_time,msg->lat_cc_block_time);
		INC_STATS(0,lat_short_cc_time,msg->lat_cc_time);
		INC_STATS(0,lat_short_process_time,msg->lat_process_time);

		if (msg->return_node_id != g_node_id) {
			/*
					if (msg->lat_network_time/BILLION > 1.0) {
							printf("%ld %d %ld -> %d: %f %f\n",msg->txn_id, msg->rtype,
					 msg->return_node_id,g_node_id ,msg->lat_network_time/BILLION,
					 msg->lat_other_time/BILLION);
					}
					*/
			INC_STATS(0,lat_short_network_time,msg->lat_network_time);
		}
		INC_STATS(0,lat_short_batch_time,wait_list[id].total_batch_time);

			PRINT_LATENCY("lat_l_seq %ld %ld %d %f %f %f\n", msg->get_txn_id(), msg->get_batch_id(),
										wait_list[id].abort_cnt, (double)timespan / BILLION,
										(double)skew_timespan / BILLION,
										(double)wait_list[id].total_batch_time / BILLION);

#if CC_ALG == HDCC || CC_ALG == SNAPPER
			if (msg->algo == CALVIN) {
#endif
					cl_msg->release();
#if CC_ALG == HDCC || CC_ALG == SNAPPER
			}
#endif

#if CC_ALG == HDCC || CC_ALG == SNAPPER
			if (msg->algo == CALVIN) {
#endif
			ClientResponseMessage *rsp_msg =
					(ClientResponseMessage *)Message::create_message(msg->get_txn_id(), CL_RSP);
					rsp_msg->client_startts = wait_list[id].client_startts;
					msg_queue.enqueue(thd_id,rsp_msg,wait_list[id].client_id);
#if CC_ALG == HDCC || CC_ALG == SNAPPER
			}
#endif
#if WORKLOAD == PPS
			}
#endif

			INC_STATS(thd_id,seq_complete_cnt,1);
	}

	// If we have all acks for this batch, send qry responses to all clients
	if (en->txns_left == 0) {
			DEBUG("FINISHED BATCH %ld\n",en->epoch);
			LIST_REMOVE_HT(en,wl_head,wl_tail);
#if CC_ALG == HDCC
			blocked = true;
			while(validationCount > 0) {}
#endif
			mem_allocator.free(en->list,sizeof(qlite) * en->max_size);
			mem_allocator.free(en,sizeof(qlite_ll));
#if CC_ALG == HDCC
			blocked = false;
#endif
	}
	INC_STATS(thd_id,seq_ack_time,get_sys_clock() - prof_stat);
}

void Sequencer::process_abort(Message *msg, uint64_t thd_id) {
	qlite_ll * en = wl_head;
	while(en != NULL && en->epoch != msg->get_batch_id()) {
		en = en->next;
	}
	assert(en);
	qlite * wait_list = en->list;
	assert(wait_list != NULL);
	assert(en->txns_left > 0);

#if CC_ALG == HDCC
	uint64_t id = (msg->get_txn_id() - en->start_txn_id) / g_node_cnt;
#else
	uint64_t id = msg->get_txn_id() / g_node_cnt;
#endif
	// recover "return node id"
	msg->return_node_id = wait_list[id].client_id;


	uint64_t prof_stat = get_sys_clock();
	assert(wait_list[id].server_ack_cnt > 0);

	en->txns_left--;
	if (en->txns_left == 0) {
		DEBUG("FINISHED BATCH %ld\n",en->epoch);
		LIST_REMOVE_HT(en,wl_head,wl_tail);
#if CC_ALG == HDCC
		blocked = true;
		while(validationCount > 0) {}
#endif
		mem_allocator.free(en->list, sizeof(qlite) * en->max_size);
		mem_allocator.free(en, sizeof(qlite_ll));
#if CC_ALG == HDCC
		blocked = false;
#endif
	}
	INC_STATS(thd_id, seq_ack_time, get_sys_clock() - prof_stat);
	
	// set it to a new client query
	msg->rtype = CL_QRY;
	process_txn(msg, thd_id, 0, 0, 0, 0);
}

// Assumes 1 thread does sequencer work
void Sequencer::process_txn(Message *msg, uint64_t thd_id, uint64_t early_start,
														uint64_t last_start, uint64_t wait_time, uint32_t abort_cnt) {

		uint64_t starttime = get_sys_clock();
		DEBUG("SEQ Processing msg\n");
		qlite_ll * en = wl_tail;

		// LL is potentially a bottleneck here
		if(!en || en->epoch != simulation->get_seq_epoch()+1) {
			DEBUG("SEQ new wait list for epoch %ld\n",simulation->get_seq_epoch()+1);
			// First txn of new wait list
			en = (qlite_ll *) mem_allocator.alloc(sizeof(qlite_ll));
			en->epoch = simulation->get_seq_epoch()+1;
			en->max_size = 1000;
			en->size = 0;
			en->txns_left = 0;
			en->list = (qlite *) mem_allocator.alloc(sizeof(qlite) * en->max_size);
#if CC_ALG == HDCC || CC_ALG == SNAPPER
			en->start_txn_id = g_node_id + g_node_cnt * next_txn_id;
#endif
			LIST_PUT_TAIL(wl_head,wl_tail,en)
		}
		if(en->size == en->max_size) {
			en->max_size *= 2;
			en->list = (qlite *) mem_allocator.realloc(en->list,sizeof(qlite) * en->max_size);
		}

		txnid_t txn_id = g_node_id + g_node_cnt * next_txn_id;
#if CC_ALG == HDCC || CC_ALG == SNAPPER
		uint64_t id = next_txn_id - last_epoch_max_id;
		next_txn_id++;
		if (id >= en->max_size) {
			en->max_size *= 2;
			en->list = (qlite *) mem_allocator.realloc(en->list,sizeof(qlite) * en->max_size);
		}
#else
		next_txn_id++;
		uint64_t id = txn_id / g_node_cnt;
#endif
		msg->batch_id = en->epoch;
		msg->txn_id = txn_id;
		assert(txn_id != UINT64_MAX);

// #if CC_ALG == HDCC
// 		if (cc_selector.get_best_cc(msg) == SILO) {
// 			msg->algo = SILO;
// 			if(msg->rtype == RTXN){
// 				msg->txn_id = msg->orig_txn_id;
// 				msg->batch_id = msg->orig_batch_id;
// 			}
// 			work_queue.enqueue(thd_id, msg, false);
// 			return;
// 		} else {
// 			msg->algo = CALVIN;
// 			msg->rtype = CL_QRY;
// 		}
#if CC_ALG == HDCC
	/*
		* force_calvin:
		*   方案二中 SILO 执行到第 k 步发现高风险后，会 abort 并通过 abort_queue
		*   重新进入 Sequencer。重试消息如果已经被标记为 CALVIN，则这里直接走 Calvin。
		*/
	bool force_calvin = (msg->rtype == RTXN && msg->algo == CALVIN);  // #RAIN 

	/*
		* predict_calvin:
		*   方案一（PREDICT_MODE==1）：事务进入 Sequencer 时直接预测和冲突检测。
		*   方案三（PREDICT_MODE==3）：事务进入 Sequencer 时用完整/干运行工作集做精确冲突检测。
		*   两者互斥，由 g_predict_mode 控制。
		*/
	bool predict_calvin = should_route_to_calvin_by_prediction(msg)     // 方案一
	                   || should_route_to_calvin_by_full_workset(msg);  // #RAIN 方案三

	if (!force_calvin &&
			!predict_calvin &&
			cc_selector.get_best_cc(msg) == SILO) {
			msg->algo = SILO;
			if(msg->rtype == RTXN){
					msg->txn_id = msg->orig_txn_id;
					msg->batch_id = msg->orig_batch_id;
			}
			work_queue.enqueue(thd_id, msg, false);
			return;
	} else {
			msg->algo = CALVIN;
			msg->rtype = CL_QRY;
	}
#elif CC_ALG == SNAPPER
		if (cc_selector.get_best_cc(msg) == WAIT_DIE) {
			msg->algo = WAIT_DIE;
			if(msg->rtype == RTXN){
				msg->txn_id = msg->orig_txn_id;
				msg->batch_id = msg->orig_batch_id;
			}
			work_queue.enqueue(thd_id, msg, false);
			return;
		} else {
			msg->algo = CALVIN;
			msg->rtype = CL_QRY;
		}
#endif

#if WORKLOAD == YCSB
		std::set<uint64_t> participants = YCSBQuery::participants(msg,_wl);
#elif WORKLOAD == TPCC
		std::set<uint64_t> participants = TPCCQuery::participants(msg,_wl);
#elif WORKLOAD == PPS
		std::set<uint64_t> participants = PPSQuery::participants(msg,_wl);
#elif WORKLOAD == DA
		std::set<uint64_t> participants = DAQuery::participants(msg,_wl);
#endif
		uint32_t server_ack_cnt = participants.size();
		assert(server_ack_cnt > 0);
		assert(ISCLIENTN(msg->get_return_id()));
		en->list[id].client_id = msg->get_return_id();
		en->list[id].client_startts = ((ClientQueryMessage*)msg)->client_startts;
		//en->list[id].seq_startts = get_sys_clock();

		en->list[id].total_batch_time = wait_time;
		en->list[id].abort_cnt = abort_cnt;
		en->list[id].skew_startts = 0;
		en->list[id].server_ack_cnt = server_ack_cnt;
		en->list[id].msg = msg;
		en->size++;
		en->txns_left++;
		// Note: Modifying msg!
		msg->return_node_id = g_node_id;
		msg->lat_network_time = 0;
		msg->lat_other_time = 0;
#if CC_ALG == CALVIN && WORKLOAD == PPS
		PPSClientQueryMessage* cl_msg = (PPSClientQueryMessage*) msg;
		if (cl_msg->txn_type == PPS_GETPARTBYSUPPLIER || cl_msg->txn_type == PPS_GETPARTBYPRODUCT ||
						cl_msg->txn_type == PPS_ORDERPRODUCT) {
				if (cl_msg->part_keys.size() == 0) {
						cl_msg->recon = true;
						en->list[id].seq_startts = get_sys_clock();
			} else {
						cl_msg->recon = false;
						en->list[id].seq_startts = last_time;
				}

		} else {
				cl_msg->recon = false;
				en->list[id].seq_startts = get_sys_clock();
		}
#else
		en->list[id].seq_startts = get_sys_clock();
#endif
		if (early_start == 0) {
				en->list[id].seq_first_startts = en->list[id].seq_startts;
		} else {
				en->list[id].seq_first_startts = early_start;
		}
		assert(en->size == en->txns_left);
		assert(en->size <= ((uint64_t)g_inflight_max * g_node_cnt));

		// Add new txn to fill queue
		for(auto participant = participants.begin(); participant != participants.end(); participant++) {
			DEBUG("SEQ adding (%ld,%ld) to fill queue (recon: %d)\n", msg->get_txn_id(),
					msg->get_batch_id(), ((PPSClientQueryMessage *)msg)->recon);
			while (!fill_queue[*participant].push(msg) && !simulation->is_done()) {
			}
		}
#if LOGGING
		char * data = (char *)malloc(sizeof(char) * 10);
		logger.writeToBuffer(thd_id, data, sizeof(data));
#endif

	INC_STATS(thd_id,seq_process_cnt,1);
	INC_STATS(thd_id,seq_process_time,get_sys_clock() - starttime);
	ATOM_ADD(total_txns_received,1);
}

// Assumes 1 thread does sequencer work
void Sequencer::send_next_batch(uint64_t thd_id) {
	uint64_t prof_stat = get_sys_clock();
	qlite_ll * en = wl_tail;
#if LOGGING
#if CC_ALG == HDCC
	logger.enqueueRecord(logger.createRecord(thd_id, L_C_FLUSH, 0, 0, 0));
#else
	logger.enqueueRecord(logger.createRecord(thd_id, L_C_FLUSH, 0, 0));
#endif
#endif
	bool empty = true;
	if(en && en->epoch == simulation->get_seq_epoch()) {
		DEBUG("SEND NEXT BATCH %ld [%ld,%ld] %ld\n", thd_id, simulation->get_seq_epoch(), en->epoch,
					en->size);
#if CC_ALG == HDCC || CC_ALG == SNAPPER
		if (en->txns_left == 0) {
			DEBUG("FINISHED BATCH %ld\n",en->epoch);
			LIST_REMOVE_HT(en,wl_head,wl_tail);
			mem_allocator.free(en->list,sizeof(qlite) * en->max_size);
			mem_allocator.free(en,sizeof(qlite_ll));
		}else{
#endif
			empty = false;
			en->batch_send_time = prof_stat;
#if CC_ALG == HDCC || CC_ALG == SNAPPER
		}
#endif
	}

	Message * msg;
	for(uint64_t j = 0; j < g_node_cnt; j++) {
		while(fill_queue[j].pop(msg)) {
			if(j == g_node_id) {
					work_queue.sched_enqueue(thd_id,msg);
			} else {
				msg_queue.enqueue(thd_id,msg,j);
			}
		}
		if(!empty) {
			DEBUG("Seq RDONE %ld\n",simulation->get_seq_epoch())
		}
		msg = Message::create_message(RDONE);
		msg->batch_id = simulation->get_seq_epoch();
		if(j == g_node_id) {
			work_queue.sched_enqueue(thd_id,msg);
		} else {
			msg_queue.enqueue(thd_id,msg,j);
		}
	}

	if(last_time_batch > 0) {
		INC_STATS(thd_id,seq_batch_time,get_sys_clock() - last_time_batch);
	}
	last_time_batch = get_sys_clock();

	INC_STATS(thd_id,seq_batch_cnt,1);
	if(!empty) {
		INC_STATS(thd_id,seq_full_batch_cnt,1);
	}
	INC_STATS(thd_id,seq_prep_time,get_sys_clock() - prof_stat);
#if CC_ALG == CALVIN
	next_txn_id = 0;
#elif CC_ALG == HDCC || CC_ALG == SNAPPER
	last_epoch_max_id = next_txn_id;
#endif
}

#if CC_ALG == HDCC
bool Sequencer::checkDependency(uint64_t batch_id, uint64_t txn_id) {
	qlite_ll * en = wl_head;
	if (!en || en->epoch > batch_id) {
		return true;
	}
	else if (en->epoch < batch_id) {
		return false;
	} else {
		if (txn_id % g_node_cnt < g_node_id) {
			return true;
		} else if (txn_id %g_node_cnt > g_node_id) {
			return false;
		} else {
			uint64_t id = (txn_id - en->start_txn_id) / g_node_cnt;
			while(blocked) {}
			ATOM_ADD(validationCount, 1);
			if (!en || !en->list || en->txns_left == 0) {
				ATOM_SUB(validationCount, 1);
				return true;
			}
			for (uint64_t i = 0; i < id || i < en->max_size; i++) {
				if (en->list[i].server_ack_cnt > 0) {
					ATOM_SUB(validationCount, 1);
					return false;
				}
			}
			ATOM_SUB(validationCount, 1);
		}
	}
	return true;
}
#endif
