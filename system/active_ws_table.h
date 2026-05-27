#pragma once

// #RAIN

#include "helper.h"
#include "generic.h"   // benchmarks/generic.h, 提供 WW_CONFLICT / RW_CONFLICT
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <mutex>

enum WsSrc : uint8_t {
    WS_PREDICTED = 0,
    WS_ACTUAL = 1
};

enum WsMode : uint8_t {
    WS_RD = 0,
    WS_WR = 1,
    WS_RW = 2,
    WS_UNK = 3
};

struct WsItem {
    uint64_t key;
    WsMode mode;
    WsSrc src;
    double conf; // predicted: (0,1], actual: 1.0
};

struct WsConflictHit {
    uint64_t other_txn_id;
    uint64_t key;
    int conflict_type; // WW_CONFLICT / RW_CONFLICT / UNKNOWN
    double risk;       // [0,1]
};

class ActiveWsTable {
public:

    ActiveWsTable();
    ~ActiveWsTable();

    void init(uint32_t shard_cnt);

    // 预测注册：txn初入系统时调用
    void register_items(uint64_t txn_id, const std::vector<WsItem> &items);

    // 实际访问更新：执行期每次拿到真实 row key + 读写类型时调用
    void upsert_actual(uint64_t txn_id, uint64_t key, WsMode mode);

    // 快速冲突评估（与"其他活跃事务"比较）
    // 返回命中列表，可用于路由决策（如转 CALVIN）
    std::vector<WsConflictHit> detect_conflicts(uint64_t txn_id, const std::vector<WsItem> &candidate) const;

    // 结束时清理
    void remove_txn(uint64_t txn_id);

private:
    struct TxnWs {
        std::unordered_map<uint64_t, WsItem> items; // key->item（实际覆盖预测）
    };

    uint32_t            _shard_cnt;

    // _txn_locks: 按 txn_id % shard 分片，保护 _txns[txn_id]
    std::mutex *        _txn_locks;

    // _key_locks: 按 key % shard 分片，保护 _key2txns[key]
    // 与 _txn_locks 独立，避免按 txn_id 加锁时无法保护跨 txn 的同 key 写入。
    // detect_conflicts 读 _key2txns / _txns 时也须持对应 key 锁。
    mutable std::mutex * _key_locks;

    // 活跃事务池：txn_id -> 工作集
    std::unordered_map<uint64_t, TxnWs> _txns;

    // 倒排索引：key -> active txn ids
    std::unordered_map<uint64_t, std::unordered_set<uint64_t> > _key2txns;

private:
    inline uint32_t lock_idx_by_txn(uint64_t txn_id) const { return txn_id % _shard_cnt; }
    inline uint32_t lock_idx_by_key(uint64_t key) const { return key % _shard_cnt; }

    static WsMode merge_mode(WsMode a, WsMode b);
    static int infer_conflict_type(WsMode a, WsMode b);
    static double infer_risk(const WsItem &a, const WsItem &b);
};