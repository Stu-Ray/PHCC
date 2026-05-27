// #RAIN

#include "workset_predictor.h"
#include <algorithm>
#include <assert.h>
#include <limits.h>

WorksetPredictor::WorksetPredictor()
    : _root(nullptr),
      _max_depth(g_k_value),
      _min_support(g_min_support),
      _shard_cnt(g_hash_lock_num),
      _dedup_in_txn(false),
      _node_cnt(0), 
      _locks(nullptr){}

WorksetPredictor::~WorksetPredictor() {
    free_subtree(_root);
    _root = nullptr;
    if (_locks) {
        delete [] _locks;
        _locks = nullptr;
    }
}

void WorksetPredictor::init(uint32_t max_depth,
                            uint32_t min_support,
                            uint32_t shard_cnt,
                            bool dedup_in_txn) {
    if (_root) {
        free_subtree(_root);
        _root = nullptr;
    }
    _max_depth      =   std::max(1u, max_depth);
    _min_support    =   std::max(1u, min_support);
    _shard_cnt      =   std::max(1u, shard_cnt);
    _dedup_in_txn   =   dedup_in_txn;

    if (_locks) {
        delete [] _locks;
        _locks = nullptr;
    }
    _locks      =       new std::mutex[_shard_cnt];

    _root       =       new Node(UINT64_MAX); // ？
    _node_cnt   =       1;
}

uint64_t WorksetPredictor::hash_context(const std::vector<uint64_t> &ctx, uint32_t begin, uint32_t end) {
    // FNV-like
    uint64_t h = 1469598103934665603ULL;
    for (uint32_t i = begin; i < end; i++) {
        h ^= ctx[i] + 0x9e3779b97f4a7c15ULL + (h << 6) + (h >> 2);
        h *= 1099511628211ULL;
    }
    return h;
}

std::vector<uint64_t> WorksetPredictor::normalize_seq(const std::vector<uint64_t> &seq, bool dedup_in_txn) {
    if (!dedup_in_txn) return seq;
    std::vector<uint64_t> out;
    out.reserve(seq.size());
    std::unordered_set<uint64_t> seen;
    for (auto x : seq) {
        if (seen.find(x) == seen.end()) {
            seen.insert(x);
            out.push_back(x);
        }
    }
    return out;
}

WorksetPredictor::Node*
WorksetPredictor::get_or_create_path_locked(const std::vector<uint64_t> &ctx, uint32_t begin, uint32_t end) {
    assert(_root);
    Node *cur = _root;
    cur->visit++;
    for (uint32_t i = begin; i < end; i++) {
        uint64_t it = ctx[i];
        auto itc = cur->children.find(it);
        if (itc == cur->children.end()) {
            Node *nn = new Node(it);
            cur->children[it] = nn;
            cur = nn;
            _node_cnt++;
        } else {
            cur = itc->second;
        }
        cur->visit++;
    }
    return cur;
}

const WorksetPredictor::Node*
WorksetPredictor::find_path(const std::vector<uint64_t> &ctx, uint32_t begin, uint32_t end) const {
    if (!_root) return nullptr;
    const Node *cur = _root;
    for (uint32_t i = begin; i < end; i++) {
        auto itc = cur->children.find(ctx[i]);
        if (itc == cur->children.end()) return nullptr;
        cur = itc->second;
    }
    return cur;
}

void WorksetPredictor::observe(const std::vector<uint64_t> &seq_raw) {
    if (!_root) return;
    std::vector<uint64_t> seq = normalize_seq(seq_raw, _dedup_in_txn);
    if (seq.size() < 2) return;

    // 对每个位置 i，更新不同阶上下文 -> next
    // 例如 A B C D:
    // [A]->B, [A B]->C, [A B C]->D, 以及 [B]->C ...
    uint32_t n = (uint32_t)seq.size();
    for (uint32_t i = 0; i + 1 < n; i++) {
        uint64_t next_item = seq[i + 1];

        // 0 阶统计：每个位置只统计一次，记录"全局下一项"频次。
        // 注意：0 阶统计使用 root 锁（shard 0），避免竞态。
        {
            uint64_t h = hash_context(seq, i, i); // 空上下文，固定 hash
            std::lock_guard<std::mutex> guard(_locks[h % _shard_cnt]);
            _root->next_freq[next_item] += 1;
        }

        uint32_t max_order_here = std::min<uint32_t>(_max_depth, i + 1);
        for (uint32_t order = 1; order <= max_order_here; order++) {
            uint32_t begin = i + 1 - order;
            uint32_t end   = i + 1;          // context = [begin, end)，即 seq[begin..i]
            // next_item = seq[end] = seq[i+1]（已在外层计算）

            uint64_t h = hash_context(seq, begin, end);
            std::mutex &lk = _locks[h % _shard_cnt];
            std::lock_guard<std::mutex> guard(lk);

            Node *ctx_node = get_or_create_path_locked(seq, begin, end);
            ctx_node->next_freq[next_item] += 1;
        }
    }
}

bool WorksetPredictor::pred_cmp(const PredItem &a, const PredItem &b) {
    if (a.freq != b.freq) return a.freq > b.freq;
    return a.item < b.item;
}

std::vector<WorksetPredictor::PredItem>
WorksetPredictor::predict(const std::vector<uint64_t> &suffix_raw, uint32_t topk) const {
    std::vector<PredItem> ans;
    if (!_root || topk == 0) return ans;

    std::vector<uint64_t> suffix = normalize_seq(suffix_raw, _dedup_in_txn);
    if (suffix.empty()) {
        // 0阶 fallback
        uint64_t sum = 0;
        for (auto &kv : _root->next_freq) sum += kv.second;
        if (sum < _min_support) return ans;

        ans.reserve(_root->next_freq.size());
        for (auto &kv : _root->next_freq) {
            PredItem p{kv.first, (double)kv.second / (double)sum, kv.second};
            ans.push_back(p);
        }
        std::sort(ans.begin(), ans.end(), pred_cmp);
        if (ans.size() > topk) ans.resize(topk);
        return ans;
    }

    uint32_t len = (uint32_t)suffix.size();
    uint32_t max_try = std::min<uint32_t>(_max_depth, len);

    // 从高阶回退到低阶
    for (int order = (int)max_try; order >= 1; --order) {
        uint32_t begin = len - (uint32_t)order;
        uint32_t end = len;

        const Node *ctx = find_path(suffix, begin, end);
        if (!ctx) continue;

        uint64_t sum = 0;
        for (auto &kv : ctx->next_freq) sum += kv.second;
        if (sum < _min_support) continue;

        ans.reserve(ctx->next_freq.size());
        for (auto &kv : ctx->next_freq) {
            PredItem p{kv.first, (double)kv.second / (double)sum, kv.second};
            ans.push_back(p);
        }
        std::sort(ans.begin(), ans.end(), pred_cmp);
        if (ans.size() > topk) ans.resize(topk);
        return ans;
    }

    // 全部miss，回退0阶
    uint64_t sum = 0;
    for (auto &kv : _root->next_freq) sum += kv.second;
    if (sum < _min_support) return ans;

    ans.reserve(_root->next_freq.size());
    for (auto &kv : _root->next_freq) {
        PredItem p{kv.first, (double)kv.second / (double)sum, kv.second};
        ans.push_back(p);
    }
    std::sort(ans.begin(), ans.end(), pred_cmp);
    if (ans.size() > topk) ans.resize(topk);
    return ans;
}

void WorksetPredictor::free_subtree(Node *n) {
    if (!n) return;
    for (auto &kv : n->children) {
        free_subtree(kv.second);
    }
    delete n;
}

uint64_t WorksetPredictor::count_subtree(Node *n) {
    if (!n) return 0;
    uint64_t cnt = 1; // n 本身
    for (auto &kv : n->children) {
        cnt += count_subtree(kv.second);
    }
    return cnt;
}

uint64_t WorksetPredictor::prune_subtree(Node *n, uint32_t min_freq_to_keep) {
    if (!n) return 0;
    uint64_t deleted = 0;

    // 先递归处理子节点
    std::vector<uint64_t> to_del;
    for (auto &kv : n->children) {
        Node *ch = kv.second;
        if (ch->visit < min_freq_to_keep) {
            // 整棵子树都要删除，统计其大小
            deleted += count_subtree(ch); // 包含 ch 本身
            to_del.push_back(kv.first);
        } else {
            // 子树保留，但内部仍可裁剪低频节点
            deleted += prune_subtree(ch, min_freq_to_keep);
        }
    }
    for (auto k : to_del) {
        Node *ch = n->children[k];
        free_subtree(ch);
        n->children.erase(k);
    }

    // next_freq 低频裁剪（不影响节点计数）
    std::vector<uint64_t> nf_del;
    for (auto &kv : n->next_freq) {
        if (kv.second < min_freq_to_keep) nf_del.push_back(kv.first);
    }
    for (auto k : nf_del) n->next_freq.erase(k);

    return deleted;
}

void WorksetPredictor::prune(uint32_t min_freq_to_keep) {
    if (!_root) return;
    if (min_freq_to_keep == 0) return;

    // 简单做法：串行全树裁剪（不并发）
    uint64_t deleted = prune_subtree(_root, min_freq_to_keep);
    if (deleted > 0 && _node_cnt >= deleted) _node_cnt -= deleted;
}

// #RAIN 导出功能实现
// CSV 格式（使用 '|' 作为 ctx_items 内部的分隔符，避免与 CSV 逗号冲突）：
// ctx_len|ctx_items|next_item|freq|node_visit
// ctx_len: 上下文长度（0 表示 root）
// ctx_items: '|' 分隔的上下文 items（空字符串表示 root）
// next_item: 下一项 item
// freq: 该 (ctx -> next_item) 的频次
// node_visit: 到达该上下文节点的次数

void WorksetPredictor::export_node_to_csv(std::ofstream &ofs, const Node *node,
                                          const std::vector<uint64_t> &ctx_items) const {
    if (!node) return;

    uint32_t ctx_len = (uint32_t)ctx_items.size();

    // 遍历该节点的所有 next_freq 条目，输出一行
    for (auto &nf_kv : node->next_freq) {
        uint64_t next_item = nf_kv.first;
        uint64_t freq = nf_kv.second;

        // 构建 ctx_items 字符串（'|' 分隔，避免与 CSV 逗号冲突）
        std::string ctx_str;
        for (size_t i = 0; i < ctx_items.size(); ++i) {
            if (i > 0) ctx_str += "|";
            ctx_str += std::to_string(ctx_items[i]);
        }

        // CSV 格式：ctx_len,ctx_items,next_item,freq,node_visit
        ofs << ctx_len << ","
            << ctx_str << ","
            << next_item << ","
            << freq << ","
            << node->visit
            << "\n";
    }

    // 递归导出子节点
    for (auto &child_kv : node->children) {
        uint64_t child_item = child_kv.first;
        const Node *child_node = child_kv.second;

        std::vector<uint64_t> child_ctx = ctx_items;
        child_ctx.push_back(child_item);

        export_node_to_csv(ofs, child_node, child_ctx);
    }
}

bool WorksetPredictor::export_to_csv(const std::string &path) const {
    if (!_root) {
        fprintf(stderr, "[WorksetPredictor] export_to_csv: predictor not initialized\n");
        return false;
    }

    std::ofstream ofs(path);
    if (!ofs.is_open()) {
        fprintf(stderr, "[WorksetPredictor] export_to_csv: failed to open file '%s'\n", path.c_str());
        return false;
    }

    // 写入 CSV 表头（注释行）
    ofs << "# WorksetPredictor Model Export\n";
    ofs << "# CSV Format (pipe '|' used as delimiter within ctx_items):\n";
    ofs << "# ctx_len,ctx_items,next_item,freq,node_visit\n";
    ofs << "# ctx_len=0 means root context (global statistics)\n";
    ofs << "# ctx_items uses '|' as internal delimiter (empty for root)\n";
    ofs << "# node_visit: visit count to this context node\n";
    ofs << "ctx_len,ctx_items,next_item,freq,node_visit\n";

    // 递归导出所有节点
    std::vector<uint64_t> empty_ctx;
    export_node_to_csv(ofs, _root, empty_ctx);

    ofs.close();

    printf("[WorksetPredictor] export_to_csv: exported %lu nodes to '%s'\n", _node_cnt, path.c_str());
    return true;
}

// #RAIN 导入功能实现
// CSV 格式（使用 '|' 作为 ctx_items 内部的分隔符）：
// ctx_len,ctx_items,next_item,freq,node_visit

bool WorksetPredictor::import_from_csv(const std::string &path) {
    std::ifstream ifs(path);
    if (!ifs.is_open()) {
        fprintf(stderr, "[WorksetPredictor] import_from_csv: failed to open file '%s'\n", path.c_str());
        return false;
    }

    // 清空现有模型
    if (_root) {
        free_subtree(_root);
        _root = nullptr;
        _node_cnt = 0;
    }

    // 重新初始化 root
    if (!_locks) {
        _locks = new std::mutex[_shard_cnt];
    }
    _root = new Node(UINT64_MAX);
    _node_cnt = 1;

    std::string line;
    uint64_t imported_edges = 0;
    uint32_t line_num = 0;
    uint32_t parse_errors = 0;

    while (std::getline(ifs, line)) {
        line_num++;

        // 跳过空行和注释行
        if (line.empty() || line[0] == '#') continue;

        // 跳过表头行
        if (line.find("ctx_len") != std::string::npos) continue;

        // 使用 '|' 作为 ctx_items 的分隔符来解析
        // 格式：ctx_len,ctx_items,next_item,freq,node_visit
        // 其中 ctx_items 内部使用 '|' 分隔

        size_t pos1 = line.find(',');
        if (pos1 == std::string::npos) continue;

        // 解析 ctx_len
        uint32_t ctx_len = 0;
        try {
            ctx_len = std::stoul(line.substr(0, pos1));
        } catch (...) {
            parse_errors++;
            continue;
        }

        // 找下一个逗号（ctx_items 结束）
        size_t pos2 = line.find(',', pos1 + 1);
        if (pos2 == std::string::npos) {
            parse_errors++;
            continue;
        }

        // 解析 ctx_items（内部使用 '|' 分隔）
        std::string ctx_str = line.substr(pos1 + 1, pos2 - pos1 - 1);
        std::vector<uint64_t> ctx_items;

        if (ctx_len > 0 && !ctx_str.empty()) {
            std::stringstream ss_ctx(ctx_str);
            std::string item_str;
            while (std::getline(ss_ctx, item_str, '|')) {
                if (item_str.empty()) continue;
                try {
                    ctx_items.push_back(std::stoull(item_str));
                } catch (...) {
                    // 跳过无效项
                }
            }
        }

        // 验证 ctx_len 与实际解析出的 items 数量是否一致
        if (ctx_items.size() != ctx_len) {
            // 以解析出的实际数量为准
            ctx_len = (uint32_t)ctx_items.size();
        }

        // 找倒数第二个逗号（next_item 和 freq 之间）
        size_t pos3 = line.find_last_of(',', line.size() - 1);
        size_t pos4 = line.find_last_of(',', pos3 - 1);
        if (pos3 == std::string::npos || pos4 == std::string::npos) {
            parse_errors++;
            continue;
        }

        // 解析 next_item, freq, node_visit
        uint64_t next_item = 0;
        uint64_t freq = 0;
        uint64_t node_visit = 0;

        try {
            next_item = std::stoull(line.substr(pos2 + 1, pos3 - pos2 - 1));
            freq = std::stoull(line.substr(pos3 + 1, pos4 - pos3 - 1));
            node_visit = std::stoull(line.substr(pos4 + 1));
        } catch (...) {
            parse_errors++;
            continue;
        }

        // 查找或创建上下文节点（带锁保护）
        Node *ctx_node = nullptr;
        if (ctx_len == 0) {
            ctx_node = _root;
            ctx_node->visit += node_visit;
        } else {
            uint64_t h = hash_context(ctx_items, 0, (uint32_t)ctx_items.size());
            std::mutex &lk = _locks[h % _shard_cnt];
            std::lock_guard<std::mutex> guard(lk);

            ctx_node = get_or_create_path_locked(ctx_items, 0, (uint32_t)ctx_items.size());
            ctx_node->visit += node_visit;
        }

        // 添加 next_freq 条目
        if (ctx_node) {
            ctx_node->next_freq[next_item] += freq;
            imported_edges++;
        }
    }

    ifs.close();

    if (parse_errors > 0) {
        fprintf(stderr, "[WorksetPredictor] import_from_csv: %u parse errors\n", parse_errors);
    }
    printf("[WorksetPredictor] import_from_csv: imported %lu edges from '%s' (nodes: %lu)\n",
           imported_edges, path.c_str(), _node_cnt);
    return true;
}