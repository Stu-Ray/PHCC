#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
#RAIN 批量性能对比测试脚本
========================
用法:
  python3 scripts/rain_bench.py                          # 使用默认配置
  python3 scripts/rain_bench.py --cc-algs TICTOC,CALVIN,HDCC --threads 4,8 --runs 3
  python3 scripts/rain_bench.py --dry-run                 # 仅打印将要执行的实验列表

功能:
  1. 自动修改 config.h 中的编译期参数（CC_ALG 等）
  2. 自动编译 (make clean && make -jN)
  3. 自动启动服务端+客户端，收集输出
  4. 从 [summary] 行解析关键指标
  5. 每组配置重复 runs 次取平均
  6. 最终输出汇总 CSV 和终端表格

注意:
  - 本脚本用于本地测试，不涉及远程部署
  - CC_ALG / WORKLOAD / PREDICT_MODE 等编译期参数通过修改 config.h 实现
  - NODE_CNT / THREAD_CNT / MAX_TXN_PER_PART 等也可通过命令行参数覆盖
"""

import os
import sys
import re
import csv
import json
import time
import signal
import shutil
import argparse
import subprocess
import itertools
from datetime import datetime
from collections import OrderedDict

# ============================================================
# 配置区
# ============================================================

# 项目根目录（脚本所在目录的上一级）
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.abspath(os.path.join(SCRIPT_DIR, ".."))
CONFIG_FILE = os.path.join(PROJECT_DIR, "config.h")

# 默认测试矩阵
DEFAULT_CC_ALGS = ["TICTOC", "SILO", "CALVIN", "HDCC"]
DEFAULT_THREADS = [4]
DEFAULT_NODE_CNT = 2
DEFAULT_RUNS = 3           # 每组配置重复次数
DEFAULT_MAKE_JOBS = 8      # 编译并行数

# 编译期参数（写入 config.h 的 #define，不支持命令行运行时设置）
# key = config.h 中的宏名, value = 要写入的值
# 可通过 --config-params 覆盖
DEFAULT_COMPILE_PARAMS = {
    "WORKLOAD":       "TPCC",
    "PREDICT_MODE":   "3",        # 仅 HDCC 有效
    "FIN_BY_TIME":    "true",
    "MAX_TXN_IN_FLIGHT": "500",
    "WARMUP":         "0",
    "ISOLATION_LEVEL": "SERIALIZABLE",
    "DONE_TIMER":     "1 * 60 * BILLION // ~1 minutes",
    "WARMUP_TIMER":   "1 * 60 * BILLION // ~1 minutes",
    "PROG_TIMER":     "10 * BILLION // in s",
    "ABORT_PENALTY":  "10 * 1000000UL   // in ns.",
    "ABORT_PENALTY_MAX": "5 * 100 * 1000000UL   // in ns.",
    "TPCC_ALL":       "1",
    "TXN_TYPE":       "TPCC_DIST",
    "PERC_PAYMENT":   "0.489",
    "MPR":            "1.0",
    "MPR_NEWORDER":   "20",
    "PREDICT_RISK_THREHOLD": "0.3",
    "MIN_PREDICT_SUPPORT": "1",
    "K_VALUE":        "3",
    "PREDICT_TOPK":   "5",
    # #RAIN IO
    "PREDICTOR_IMPORT_ENABLE": "0",
    "PREDICTOR_EXPORT_ENABLE": "0",
}

# 命令行参数（rundb/runcl 的 -flag 参数，可运行时设置）
# key = 宏名, flag = 命令行参数前缀
DEFAULT_RUNTIME_PARAMS = {
    "THREAD_CNT":             "-t",
    "REM_THREAD_CNT":         "-tr",
    "SEND_THREAD_CNT":        "-ts",
    "CLIENT_THREAD_CNT":      "-ct",
    "CLIENT_REM_THREAD_CNT":  "-ctr",
    "CLIENT_SEND_THREAD_CNT": "-cts",
    "NODE_CNT":               "-n",
    "MAX_TXN_PER_PART":       "-tpp",
    "MAX_TXN_IN_FLIGHT":      "-tif",
}

# 运行时参数的默认值（当不在编译期写入 config.h 时使用）
DEFAULT_RUNTIME_VALUES = {
    "THREAD_CNT":             "4",
    "REM_THREAD_CNT":         "2",
    "SEND_THREAD_CNT":        "2",
    "CLIENT_THREAD_CNT":      "4",
    "CLIENT_REM_THREAD_CNT":  "2",
    "CLIENT_SEND_THREAD_CNT": "2",
    "NODE_CNT":               "2",
    "MAX_TXN_PER_PART":       "500000",
    "MAX_TXN_IN_FLIGHT":      "500",
}

# #RAIN 专属命令行参数（仅 HDCC 时追加）
RAIN_RUNTIME_FLAGS = {
    "rk":   "K_VALUE",        # -rkINT
    "rtopk": "PREDICT_TOPK",  # -rtopkINT
    "rrisk": "PREDICT_RISK_THREHOLD",  # -rriskFLOAT
    "rms":  "MIN_PREDICT_SUPPORT",     # -rmsINT
}

# 要从 [summary] 行中提取的关键指标
SUMMARY_KEYWORDS = [
    "total_runtime",
    "tput",
    "txn_cnt",
    "total_txn_commit_cnt",
    "total_txn_abort_cnt",
    "local_txn_commit_cnt",
    "remote_txn_commit_cnt",
    "local_txn_abort_cnt",
    "remote_txn_abort_cnt",
    "txn_run_time",
    "txn_run_avg_time",
    "multi_part_txn_cnt",
    "single_part_txn_cnt",
]

# 额外的 HDCC 关键词
HDCC_KEYWORDS = [
    "hdcc_silo_cnt",
    "hdcc_calvin_cnt",
    "hdcc_silo_local_cnt",
    "hdcc_calvin_local_cnt",
    "saved_txn_cnt",
    "deterministic_abort_cnt_silo",
    "deterministic_abort_cnt_calvin",
]

# 延迟关键词
LATENCY_KEYWORDS = [
    "lscl50", "lscl99", "lscl_avg",
    "ccl50",  "ccl99",
]


# ============================================================
# 工具函数
# ============================================================

def log(msg, level="INFO"):
    ts = datetime.now().strftime("%H:%M:%S")
    prefix = {"INFO": "\033[94m", "OK": "\033[92m",
              "WARN": "\033[93m", "ERR": "\033[91m",
              "BOLD": "\033[1m", "END": "\033[0m"}.get(level, "")
    end = "\033[0m" if level in ("INFO", "OK", "WARN", "ERR", "BOLD") else ""
    print(f"{prefix}[{ts}] {msg}{end}")


def backup_config():
    """备份 config.h"""
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup = CONFIG_FILE + f".bak_{ts}"
    shutil.copy2(CONFIG_FILE, backup)
    log(f"config.h backed up to {backup}")
    return backup


def modify_config(compile_params):
    """
    修改 config.h 中的编译期参数。
    compile_params: dict { "CC_ALG": "TICTOC", ... }
    """
    with open(CONFIG_FILE, "r") as f:
        lines = f.readlines()

    new_lines = []
    for line in lines:
        modified = False
        for key, val in compile_params.items():
            # 匹配 #define KEY 后跟空白或制表符
            pattern = r"^(\s*#\s*define\s+" + re.escape(key) + r")\s+.*$"
            m = re.match(pattern, line)
            if m:
                new_lines.append(f"#define {key} {val}\n")
                modified = True
                break
        if not modified:
            new_lines.append(line)

    with open(CONFIG_FILE, "w") as f:
        f.writelines(new_lines)


def run_make(jobs):
    """编译项目"""
    cmd = f"make clean && make deps && make -j"
    log(f"Compiling: {cmd}")
    result = subprocess.run(
        cmd, shell=True, cwd=PROJECT_DIR,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
    )
    if result.returncode != 0:
        log(f"Compilation failed!\n{result.stderr[-500:]}", "ERR")
        return False
    log("Compilation successful", "OK")
    return True


def run_experiment(node_cnt, extra_args, output_dir):
    """
    启动所有节点并等待完成。
    返回 (success, server_outputs, client_outputs) 列表。
    """
    rundb = os.path.join(PROJECT_DIR, "rundb")
    runcl = os.path.join(PROJECT_DIR, "runcl")

    # 确保可执行文件存在
    if not os.path.isfile(rundb) or not os.path.isfile(runcl):
        log(f"rundb or runcl not found in {PROJECT_DIR}", "ERR")
        return False, [], []

    os.makedirs(output_dir, exist_ok=True)

    processes = []
    server_outfiles = []
    client_outfiles = []

    # 启动服务端节点
    for n in range(node_cnt):
        cmd = [rundb, f"-nid{n}"] + extra_args
        outfile_path = os.path.join(output_dir, f"server_{n}.out")
        server_outfiles.append(outfile_path)
        log(f"  Starting server node {n}: {' '.join(cmd)}")
        with open(outfile_path, "w") as of:
            p = subprocess.Popen(cmd, stdout=of, stderr=subprocess.STDOUT,
                                 cwd=PROJECT_DIR)
            processes.append(p)

    # 等待服务端先启动
    time.sleep(3)

    # 启动客户端节点
    cl_node_cnt = node_cnt  # CLIENT_NODE_CNT == NODE_CNT
    for n in range(cl_node_cnt):
        node_id = node_cnt + n
        cmd = [runcl, f"-nid{node_id}"] + extra_args
        outfile_path = os.path.join(output_dir, f"client_{n}.out")
        client_outfiles.append(outfile_path)
        log(f"  Starting client node {n}: {' '.join(cmd)}")
        with open(outfile_path, "w") as of:
            p = subprocess.Popen(cmd, stdout=of, stderr=subprocess.STDOUT,
                                 cwd=PROJECT_DIR)
            processes.append(p)

    # 等待所有进程完成
    try:
        for p in processes:
            p.wait(timeout=600)  # 最多等 10 分钟
    except subprocess.TimeoutExpired:
        log("Timeout waiting for processes!", "WARN")
        for p in processes:
            p.kill()
        return False, server_outfiles, client_outfiles

    # 检查退出码
    for i, p in enumerate(processes):
        if p.returncode != 0:
            log(f"  Process {i} exited with code {p.returncode}", "WARN")

    return True, server_outfiles, client_outfiles


def parse_summary_line(content):
    """
    从 [summary] 行中提取 key=value 对。
    返回 dict。
    """
    result = {}
    # 找到 [summary] 行
    for line in content.split("\n"):
        if "[summary]" in line:
            # 去掉 [summary] 前缀
            line = line.split("[summary]")[-1].strip()
            parts = line.split(",")
            for part in parts:
                part = part.strip()
                if "=" in part:
                    key, val = part.split("=", 1)
                    key = key.strip()
                    val = val.strip()
                    try:
                        if "." in val:
                            result[key] = float(val)
                        else:
                            result[key] = int(val)
                    except ValueError:
                        result[key] = val
            break
    return result


def extract_metrics(server_files, cc_alg):
    """
    从所有服务端输出文件中提取指标并聚合。
    返回 dict { metric_name: aggregated_value }。
    """
    all_metrics = {}

    for fpath in server_files:
        if not os.path.isfile(fpath):
            continue
        with open(fpath, "r") as f:
            content = f.read()
        metrics = parse_summary_line(content)
        if not metrics:
            continue
        for k, v in metrics.items():
            if k not in all_metrics:
                all_metrics[k] = []
            all_metrics[k].append(v)

    # 聚合策略
    aggregated = {}
    SUM_KEYS = {"txn_cnt", "total_txn_commit_cnt", "total_txn_abort_cnt",
                "local_txn_commit_cnt", "remote_txn_commit_cnt",
                "local_txn_abort_cnt", "remote_txn_abort_cnt",
                "multi_part_txn_cnt", "single_part_txn_cnt",
                "msg_send_cnt", "msg_recv_cnt",
                "txn_write_cnt", "record_write_cnt"}
    AVG_KEYS = {"total_runtime", "tput", "txn_run_time", "txn_run_avg_time",
                "lscl50", "lscl99", "lscl_avg", "ccl50", "ccl99"}

    if cc_alg == "HDCC":
        SUM_KEYS.update({"hdcc_silo_cnt", "hdcc_calvin_cnt",
                         "hdcc_silo_local_cnt", "hdcc_calvin_local_cnt",
                         "saved_txn_cnt",
                         "deterministic_abort_cnt_silo", "deterministic_abort_cnt_calvin"})

    for k, values in all_metrics.items():
        if not values:
            continue
        try:
            nums = [float(v) for v in values]
        except (ValueError, TypeError):
            aggregated[k] = values[0]
            continue
        if k in SUM_KEYS:
            aggregated[k] = sum(nums)
        elif k in AVG_KEYS:
            aggregated[k] = sum(nums) / len(nums)
        else:
            # 默认取平均
            aggregated[k] = sum(nums) / len(nums)

    return aggregated


def compute_derived(metrics):
    """计算派生指标"""
    derived = {}
    commit = metrics.get("total_txn_commit_cnt", 0)
    abort = metrics.get("total_txn_abort_cnt", 0)
    total = commit + abort
    derived["commit_rate"] = (commit / total * 100.0) if total > 0 else 0.0
    derived["abort_rate"] = (abort / total * 100.0) if total > 0 else 0.0

    # HDCC 路由比例
    silo = metrics.get("hdcc_silo_cnt", 0)
    calvin = metrics.get("hdcc_calvin_cnt", 0)
    routed = silo + calvin
    derived["silo_pct"] = (silo / routed * 100.0) if routed > 0 else 0.0
    derived["calvin_pct"] = (calvin / routed * 100.0) if routed > 0 else 0.0

    return derived


# ============================================================
# 主流程
# ============================================================

def build_experiment_matrix(args):
    """
    构建实验矩阵。
    返回 list of dict, 每个 dict 是一组实验配置。
    """
    cc_algs = [x.strip().upper() for x in args.cc_algs.split(",")]
    thread_list = [int(x.strip()) for x in args.threads.split(",")]
    runs = args.runs

    experiments = []
    for cc_alg in cc_algs:
        for threads in thread_list:
            for run_idx in range(1, runs + 1):
                exp = {
                    "cc_alg": cc_alg,
                    "threads": threads,
                    "node_cnt": args.node_cnt,
                    "run": run_idx,
                }
                experiments.append(exp)

    return experiments


def build_compile_params(exp):
    """为一组实验构建编译期参数"""
    params = dict(DEFAULT_COMPILE_PARAMS)
    params["CC_ALG"] = exp["cc_alg"]

    # NODE_CNT 需要写入 config.h（因为很多参数依赖它）
    params["NODE_CNT"] = str(exp["node_cnt"])
    params["THREAD_CNT"] = str(exp["threads"])
    params["REM_THREAD_CNT"] = str(exp["node_cnt"])
    params["SEND_THREAD_CNT"] = str(exp["node_cnt"])
    params["CORE_CNT"] = str(exp["node_cnt"])
    params["CLIENT_NODE_CNT"] = "NODE_CNT"
    params["CLIENT_REM_THREAD_CNT"] = str(exp["node_cnt"])
    params["CLIENT_SEND_THREAD_CNT"] = str(exp["node_cnt"])

    return params


def build_runtime_args(exp):
    """构建 rundb/runcl 的命令行参数"""
    args = []
    n = exp["node_cnt"]

    # 基础运行时参数
    runtime_flags = {
        "THREAD_CNT": str(exp["threads"]),
        "REM_THREAD_CNT": str(n),
        "SEND_THREAD_CNT": str(n),
        "CLIENT_THREAD_CNT": str(exp["threads"]),
        "CLIENT_REM_THREAD_CNT": str(n),
        "CLIENT_SEND_THREAD_CNT": str(n),
        "NODE_CNT": str(n),
    }

    for param, flag in DEFAULT_RUNTIME_PARAMS.items():
        if param in runtime_flags:
            args.extend([flag, runtime_flags[param]])

    return args


def main():
    parser = argparse.ArgumentParser(
        description="#RAIN Batch Benchmark Script",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # 默认配置（TICTOC, SILO, CALVIN, HDCC 各跑 3 次）
  python3 scripts/rain_bench.py

  # 指定算法和线程数
  python3 scripts/rain_bench.py --cc-algs TICTOC,HDCC --threads 4,8,16

  # 只测试 HDCC，5 次重复
  python3 scripts/rain_bench.py --cc-algs HDCC --runs 5

  # 干跑模式（仅显示将要执行的实验，不实际运行）
  python3 scripts/rain_bench.py --dry-run

  # 指定节点数和编译参数
  python3 scripts/rain_bench.py --node-cnt 4 --config CC_ALG=HDCC PREDICT_MODE=3
        """
    )
    parser.add_argument("--cc-algs", type=str, default=",".join(DEFAULT_CC_ALGS),
                        help=f"要测试的 CC 算法列表，逗号分隔 (默认: {','.join(DEFAULT_CC_ALGS)})")
    parser.add_argument("--threads", type=str, default=",".join(str(x) for x in DEFAULT_THREADS),
                        help=f"线程数列表，逗号分隔 (默认: {','.join(str(x) for x in DEFAULT_THREADS)})")
    parser.add_argument("--node-cnt", type=int, default=DEFAULT_NODE_CNT,
                        help=f"分区/节点数 (默认: {DEFAULT_NODE_CNT})")
    parser.add_argument("--runs", type=int, default=DEFAULT_RUNS,
                        help=f"每组配置重复次数 (默认: {DEFAULT_RUNS})")
    parser.add_argument("--make-jobs", type=int, default=DEFAULT_MAKE_JOBS,
                        help=f"编译并行数 (默认: {DEFAULT_MAKE_JOBS})")
    parser.add_argument("--output-dir", type=str, default=None,
                        help="结果输出目录 (默认: results/YYYYMMDD-HHMMSS/)")
    parser.add_argument("--dry-run", action="store_true",
                        help="仅打印实验列表，不执行")
    parser.add_argument("--config", nargs="*", metavar="KEY=VAL",
                        help="覆盖编译期 config.h 参数，如 --config PREDICT_MODE=3 MPR=0.5")
    parser.add_argument("--no-backup", action="store_true",
                        help="不备份 config.h")
    parser.add_argument("--skip-compile", action="store_true",
                        help="跳过编译（假设已经编译好了当前 CC_ALG）")

    args = parser.parse_args()

    # 处理 --config 覆盖
    user_config_overrides = {}
    if args.config:
        for item in args.config:
            if "=" in item:
                k, v = item.split("=", 1)
                user_config_overrides[k.strip()] = v.strip()

    # 构建实验矩阵
    experiments = build_experiment_matrix(args)

    # 输出目录
    if args.output_dir:
        output_base = args.output_dir
    else:
        output_base = os.path.join(PROJECT_DIR, "results",
                                   datetime.now().strftime("%Y%m%d-%H%M%S"))
    os.makedirs(output_base, exist_ok=True)

    # 保存实验配置
    config_info = {
        "cc_algs": [x.strip().upper() for x in args.cc_algs.split(",")],
        "threads": [int(x.strip()) for x in args.threads.split(",")],
        "node_cnt": args.node_cnt,
        "runs": args.runs,
        "user_overrides": user_config_overrides,
        "total_experiments": len(experiments),
    }
    with open(os.path.join(output_base, "config.json"), "w") as f:
        json.dump(config_info, f, indent=2)

    # 打印实验矩阵
    log("=" * 60, "BOLD")
    log("#RAIN Batch Benchmark", "BOLD")
    log("=" * 60, "BOLD")
    log(f"  CC_ALGs:   {config_info['cc_algs']}")
    log(f"  Threads:   {config_info['threads']}")
    log(f"  NodeCnt:   {args.node_cnt}")
    log(f"  Runs:      {args.runs}")
    log(f"  Total exp: {len(experiments)}")
    log(f"  Output:    {output_base}")
    if user_config_overrides:
        log(f"  Overrides: {user_config_overrides}")
    log("=" * 60, "BOLD")

    if args.dry_run:
        print("\nExperiments to run:")
        print(f"  {'CC_ALG':<10} {'Threads':>8} {'Nodes':>6} {'Run':>4}")
        print("  " + "-" * 34)
        for exp in experiments:
            print(f"  {exp['cc_alg']:<10} {exp['threads']:>8} {exp['node_cnt']:>6} {exp['run']:>4}")
        print(f"\n  Total: {len(experiments)} experiments")
        return

    # 备份 config.h
    backup_path = None
    if not args.no_backup:
        backup_path = backup_config()

    # 按需编译缓存：同一 CC_ALG 只编译一次
    compiled_alg = None
    all_results = []  # list of { cc_alg, threads, run, metrics, derived }

    try:
        for i, exp in enumerate(experiments):
            tag = f"[{i+1}/{len(experiments)}] {exp['cc_alg']}-T{exp['threads']}-R{exp['run']}"
            log(tag, "BOLD")

            exp_dir = os.path.join(output_base,
                                   f"{exp['cc_alg']}_T{exp['threads']}_R{exp['run']}")

            # 编译（同一 CC_ALG 只编译一次）
            if exp["cc_alg"] != compiled_alg and not args.skip_compile:
                compile_params = build_compile_params(exp)
                # 应用用户覆盖
                compile_params.update(user_config_overrides)
                modify_config(compile_params)
                if not run_make(args.make_jobs):
                    log(f"Skipping {tag} due to compile error", "ERR")
                    continue
                compiled_alg = exp["cc_alg"]

            # 运行
            runtime_args = build_runtime_args(exp)
            success, server_files, client_files = run_experiment(
                exp["node_cnt"], runtime_args, exp_dir
            )

            if not success:
                log(f"{tag}: FAILED", "ERR")
                continue

            # 解析结果
            metrics = extract_metrics(server_files, exp["cc_alg"])
            derived = compute_derived(metrics)

            result = {
                "cc_alg": exp["cc_alg"],
                "threads": exp["threads"],
                "node_cnt": exp["node_cnt"],
                "run": exp["run"],
                "metrics": metrics,
                "derived": derived,
            }
            all_results.append(result)

            # 打印单次结果摘要
            tput = metrics.get("tput", 0)
            commit_rate = derived.get("commit_rate", 0)
            abort_rate = derived.get("abort_rate", 0)
            runtime = metrics.get("total_runtime", 0)
            log(f"{tag}: tput={tput:.1f} txn/s, commit={commit_rate:.1f}%, "
                f"abort={abort_rate:.1f}%, time={runtime:.1f}s", "OK")

            # 保存单次原始数据
            raw_file = os.path.join(exp_dir, "parsed_metrics.json")
            with open(raw_file, "w") as f:
                json.dump({"metrics": metrics, "derived": derived}, f, indent=2)

    except KeyboardInterrupt:
        log("Interrupted by user", "WARN")
    finally:
        # 恢复 config.h
        if backup_path and os.path.isfile(backup_path):
            shutil.copy2(backup_path, CONFIG_FILE)
            log(f"config.h restored from backup")
            os.remove(backup_path)

    # ============================================================
    # 汇总 & 输出
    # ============================================================
    if not all_results:
        log("No results collected", "ERR")
        return

    # 按 (cc_alg, threads) 分组，计算平均值
    groups = OrderedDict()
    for r in all_results:
        key = (r["cc_alg"], r["threads"])
        if key not in groups:
            groups[key] = []
        groups[key].append(r)

    # 汇总表
    summary_rows = []
    # 表头
    header = ["CC_ALG", "Threads", "Nodes", "Runs",
              "Throughput(txn/s)", "Runtime(s)",
              "Txn_Total", "Committed", "Commit%",
              "Aborted", "Abort%",
              "P50_Lat(ms)", "P99_Lat(ms)"]

    # HDCC 额外列
    has_hdcc = any(r["cc_alg"] == "HDCC" for r in all_results)
    if has_hdcc:
        header.extend(["Silo%", "Calvin%", "Saved"])

    # 数值列索引（需要计算平均和标准差）
    numeric_cols = header[4:]

    for (cc_alg, threads), runs_data in groups.items():
        row = {
            "CC_ALG": cc_alg,
            "Threads": threads,
            "Nodes": runs_data[0]["node_cnt"],
            "Runs": len(runs_data),
        }

        # 收集每次运行的数值
        col_values = {col: [] for col in numeric_cols}
        for r in runs_data:
            m = r["metrics"]
            d = r["derived"]
            col_values["Throughput(txn/s)"].append(m.get("tput", 0))
            col_values["Runtime(s)"].append(m.get("total_runtime", 0))
            col_values["Txn_Total"].append(m.get("txn_cnt", 0))
            col_values["Committed"].append(m.get("total_txn_commit_cnt", 0))
            col_values["Commit%"].append(d.get("commit_rate", 0))
            col_values["Aborted"].append(m.get("total_txn_abort_cnt", 0))
            col_values["Abort%"].append(d.get("abort_rate", 0))
            col_values["P50_Lat(ms)"].append(m.get("lscl50", 0) * 1000)
            col_values["P99_Lat(ms)"].append(m.get("lscl99", 0) * 1000)
            if has_hdcc:
                col_values["Silo%"].append(d.get("silo_pct", 0))
                col_values["Calvin%"].append(d.get("calvin_pct", 0))
                col_values["Saved"].append(m.get("saved_txn_cnt", 0))

        # 计算平均和标准差
        for col in numeric_cols:
            vals = col_values[col]
            if vals:
                avg = sum(vals) / len(vals)
                if len(vals) > 1:
                    stdev = (sum((v - avg)**2 for v in vals) / len(vals)) ** 0.5
                else:
                    stdev = 0
                row[col] = f"{avg:.1f} +/- {stdev:.1f}"
            else:
                row[col] = "N/A"

        summary_rows.append(row)

    # 写入 CSV
    csv_path = os.path.join(output_base, "summary.csv")
    with open(csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()
        for row in summary_rows:
            writer.writerow(row)
    log(f"Summary CSV: {csv_path}")

    # 打印终端表格
    log("=" * 100, "BOLD")
    log("#RAIN BENCHMARK RESULTS", "BOLD")
    log("=" * 100, "BOLD")

    # 计算列宽
    col_widths = []
    for col in header:
        w = max(len(col), 10)
        for row in summary_rows:
            w = max(w, len(str(row.get(col, ""))))
        col_widths.append(min(w, 20))

    def fmt_row(row_dict, is_header=False):
        parts = []
        for col, w in zip(header, col_widths):
            val = str(row_dict.get(col, col))
            if is_header:
                parts.append(val.center(w))
            else:
                parts.append(val.rjust(w))
        return "| " + " | ".join(parts) + " |"

    def sep_row():
        parts = ["-" * w for w in col_widths]
        return "+" + "+".join(parts) + "+"

    print()
    print(sep_row())
    print(fmt_row({col: col for col in header}, is_header=True))
    print(sep_row())
    for row in summary_rows:
        print(fmt_row(row))
        print(sep_row())

    # 也输出纯数值版（方便复制到 Excel）
    csv_clean_path = os.path.join(output_base, "summary_clean.csv")
    with open(csv_clean_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        for row in summary_rows:
            clean_row = []
            for col in header:
                val = row.get(col, "")
                # 去掉 +/- 标准差，只保留平均值
                if "+/-" in str(val):
                    clean_row.append(str(val).split("+/-")[0].strip())
                else:
                    clean_row.append(val)
            writer.writerow(clean_row)

    log(f"Clean CSV (no stdev): {csv_clean_path}")
    log("Done!", "OK")


if __name__ == "__main__":
    main()
