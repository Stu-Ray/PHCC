// #RAIN

#include "active_ws_table.h"
#include <algorithm>

ActiveWsTable::ActiveWsTable()
    : _shard_cnt(g_hash_lock_num),
      _txn_locks(nullptr),
      _key_locks(nullptr) {}

ActiveWsTable::~ActiveWsTable() {
    if (_txn_locks) {
        delete [] _txn_locks;
        _txn_locks = nullptr;
    }
    if (_key_locks) {
        delete [] _key_locks;
        _key_locks = nullptr;
    }
}

void ActiveWsTable::init(uint32_t shard_cnt) {
    _shard_cnt = std::max<uint32_t>(1, shard_cnt);

    if (_txn_locks) { delete [] _txn_locks; _txn_locks = nullptr; }
    if (_key_locks) { delete [] _key_locks; _key_locks = nullptr; }

    _txn_locks  = new std::mutex[_shard_cnt];
    _key_locks  = new std::mutex[_shard_cnt];

    _txns.clear();
    _key2txns.clear();
}

WsMode ActiveWsTable::merge_mode(WsMode a, WsMode b) {
    if (a == b) return a;
    if (a == WS_UNK) return b;
    if (b == WS_UNK) return a;
    return WS_RW; // RD+WR => RW
}

int ActiveWsTable::infer_conflict_type(WsMode a, WsMode b) {
    bool a_wr = (a == WS_WR || a == WS_RW || a == WS_UNK);
    bool b_wr = (b == WS_WR || b == WS_RW || b == WS_UNK);
    if (a_wr && b_wr) return WW_CONFLICT;
    if (a_wr || b_wr) return RW_CONFLICT;
    return UNKNOWN;
}

double ActiveWsTable::infer_risk(const WsItem &a, const WsItem &b) {
    // actual=1.0，predicted=conf
    double pa = (a.src == WS_ACTUAL ? 1.0 : a.conf);
    double pb = (b.src == WS_ACTUAL ? 1.0 : b.conf);
    double r = pa * pb;
    if (r < 0) r = 0;
    if (r > 1) r = 1;
    return r;
}

void ActiveWsTable::register_items(uint64_t txn_id, const std::vector<WsItem> &items) {
    /*
     * 加锁顺序：先 txn 锁（写 _txns），再逐 key 加 key 锁（写 _key2txns）。
     * 注意持有 txn 锁期间不得再试图获取同一 shard 的 txn 锁，避免死锁。
     * key 锁与 txn 锁属于不同锁域，交叉持有是安全的。
     */
    std::lock_guard<std::mutex> tg(_txn_locks[lock_idx_by_txn(txn_id)]);
    TxnWs &txn_ws = _txns[txn_id];

    for (auto &it : items) {
        // 更新 _txns（已持 txn 锁）
        auto iter = txn_ws.items.find(it.key);
        if (iter == txn_ws.items.end()) {
            txn_ws.items[it.key] = it;
        } else {
            WsItem &old = iter->second;
            if (old.src == WS_ACTUAL) {
                old.mode = merge_mode(old.mode, it.mode);
                old.conf = 1.0;
            } else {
                old.mode = merge_mode(old.mode, it.mode);
                old.conf = std::max(old.conf, it.conf);
            }
        }

        // 更新 _key2txns（持 key 锁）
        std::lock_guard<std::mutex> kg(_key_locks[lock_idx_by_key(it.key)]);
        _key2txns[it.key].insert(txn_id);
    }
}

void ActiveWsTable::upsert_actual(uint64_t txn_id, uint64_t key, WsMode mode) {
    std::lock_guard<std::mutex> tg(_txn_locks[lock_idx_by_txn(txn_id)]);
    TxnWs &txn_ws = _txns[txn_id];

    auto iter = txn_ws.items.find(key);
    if (iter == txn_ws.items.end()) {
        WsItem it;
        it.key  = key;
        it.mode = mode;
        it.src  = WS_ACTUAL;
        it.conf = 1.0;
        txn_ws.items[key] = it;
    } else {
        WsItem &old = iter->second;
        old.src  = WS_ACTUAL;
        old.mode = merge_mode(old.mode, mode);
        old.conf = 1.0;
    }

    // 更新倒排索引（持 key 锁）
    std::lock_guard<std::mutex> kg(_key_locks[lock_idx_by_key(key)]);
    _key2txns[key].insert(txn_id);
}

std::vector<WsConflictHit> ActiveWsTable::detect_conflicts(
    uint64_t txn_id, const std::vector<WsItem> &candidate) const {

    std::vector<WsConflictHit> out;
    std::unordered_set<uint64_t> visited_pair;

    for (auto &c : candidate) {
        // 持 key 锁读取 _key2txns，防止并发写入导致 data race
        std::unordered_set<uint64_t> other_txns;
        {
            std::lock_guard<std::mutex> kg(_key_locks[lock_idx_by_key(c.key)]);
            auto it = _key2txns.find(c.key);
            if (it == _key2txns.end()) continue;
            other_txns = it->second; // 快照，释放锁后遍历
        }

        for (auto other_txn : other_txns) {
            if (other_txn == txn_id) continue;

            // 去重：同一 (other_txn, key) 对只报告一次
            uint64_t pair_key = (other_txn << 32) ^ c.key;
            if (visited_pair.count(pair_key)) continue;
            visited_pair.insert(pair_key);

            // 持对方 txn 锁读取其工作集
            WsItem o;
            bool found = false;
            {
                std::lock_guard<std::mutex> tg(
                    _txn_locks[lock_idx_by_txn(other_txn)]);
                auto tx_it = _txns.find(other_txn);
                if (tx_it == _txns.end()) continue;
                auto item_it = tx_it->second.items.find(c.key);
                if (item_it == tx_it->second.items.end()) continue;
                o     = item_it->second;
                found = true;
            }
            if (!found) continue;

            int ctype = infer_conflict_type(c.mode, o.mode);
            if (ctype == UNKNOWN) continue;

            WsConflictHit hit;
            hit.other_txn_id  = other_txn;
            hit.key           = c.key;
            hit.conflict_type = ctype;
            hit.risk          = infer_risk(c, o);
            out.push_back(hit);
        }
    }
    return out;
}

void ActiveWsTable::remove_txn(uint64_t txn_id) {
    std::lock_guard<std::mutex> tg(_txn_locks[lock_idx_by_txn(txn_id)]);
    auto tx_it = _txns.find(txn_id);
    if (tx_it == _txns.end()) return;

    for (auto &kv : tx_it->second.items) {
        std::lock_guard<std::mutex> kg(_key_locks[lock_idx_by_key(kv.first)]);
        auto key_it = _key2txns.find(kv.first);
        if (key_it != _key2txns.end()) {
            key_it->second.erase(txn_id);
            if (key_it->second.empty()) _key2txns.erase(key_it);
        }
    }
    _txns.erase(tx_it);
}
