#pragma once

#ifndef _WORKSET_PREDICTOR_H_
#define _WORKSET_PREDICTOR_H_

// #RAIN

#include "global.h"
#include "message.h"
#include "helper.h"
#include "tpcc_helper.h"
#include "tpcc_query.h"

#include <unordered_map>
#include <unordered_set>
#include <mutex>
#include <utility>
#include <fstream>
#include <sstream>

class WorksetPredictor {
public:
    struct PredItem {
        uint64_t item;
        double prob;
        uint64_t freq;
    };

    WorksetPredictor();
    ~WorksetPredictor();

    // 必须先调用
    void init(uint32_t max_depth,
              uint32_t min_support,
              uint32_t _shard_cnt,
              bool dedup_in_txn);

    // 训练：输入一个事务工作集序列（例如 A,B,C,D,E）
    void observe(const std::vector<uint64_t> &seq);

    // 预测：给后缀上下文，返回 top-k 下一项
    // suffix 例如 [B,C,D]，表示预测 D 之后最可能出现什么
    std::vector<PredItem> predict(const std::vector<uint64_t> &suffix,
                                  uint32_t topk) const;

    // 可选：周期性裁剪低频分支，避免内存膨胀
    void prune(uint32_t min_freq_to_keep);

    // #RAIN 导入导出功能
    // 导出：将当前预测器模型导出到 CSV 文件
    // CSV 格式：ctx_len,ctx_items,next_item,freq,node_visit
    //   - ctx_len: 上下文长度（0 表示 root）
    //   - ctx_items: 逗号分隔的上下文 items（空字符串表示 root）
    //   - next_item: 下一项 item
    //   - freq: 该 (ctx -> next_item) 的频次
    //   - node_visit: 到达该上下文节点的次数
    bool export_to_csv(const std::string &path) const;

    // 导入：从 CSV 文件加载预测器模型（会清空现有模型后重建）
    // 返回值：true=成功，false=失败
    bool import_from_csv(const std::string &path);

    // debug
    uint64_t node_count() const { return _node_cnt; }

private:
    struct Node {
        uint64_t item;                  // 该节点对应的数据项（root 用 UINT64_MAX）
        uint64_t visit;                 // 到达该节点次数
        std::unordered_map<uint64_t, uint64_t> next_freq; // 下一项频次
        std::unordered_map<uint64_t, Node*> children;     // 子节点
        Node(uint64_t it) : item(it), visit(0) {}
    };

    Node *      _root;
    uint32_t    _max_depth;
    uint32_t    _min_support;
    uint32_t    _shard_cnt;
    bool        _dedup_in_txn;
    uint64_t    _node_cnt;

    // 分片锁：按 context hash 上锁（避免全局大锁）
    // mutable std::vector<std::mutex> _locks;
    std::mutex * _locks; 

    // helper
    static uint64_t hash_context(const std::vector<uint64_t> &ctx, uint32_t begin, uint32_t end);
    static std::vector<uint64_t> normalize_seq(const std::vector<uint64_t> &seq, bool dedup_in_txn);
    static bool pred_cmp(const PredItem &a, const PredItem &b);

    Node* get_or_create_path_locked(const std::vector<uint64_t> &ctx, uint32_t begin, uint32_t end);
    const Node* find_path(const std::vector<uint64_t> &ctx, uint32_t begin, uint32_t end) const;

    static void free_subtree(Node *n);
    static uint64_t count_subtree(Node *n);                             // 返回以 n 为根的子树节点总数（含 n）
    static uint64_t prune_subtree(Node *n, uint32_t min_freq_to_keep); // 返回删除节点总数

    // #RAIN 导入导出辅助函数
    void export_node_to_csv(std::ofstream &ofs, const Node *node,
                            const std::vector<uint64_t> &ctx_items) const;
};

#endif
