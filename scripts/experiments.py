import itertools
# Experiments to run and analyze
# Go to end of file to fill in experiments
SHORTNAMES = {
    "CLIENT_NODE_CNT" : "CN",
    "CLIENT_THREAD_CNT" : "CT",
    "CLIENT_REM_THREAD_CNT" : "CRT",
    "CLIENT_SEND_THREAD_CNT" : "CST",
    "NODE_CNT" : "N",
    "THREAD_CNT" : "T",
    "REM_THREAD_CNT" : "RT",
    "SEND_THREAD_CNT" : "ST",
    "CC_ALG" : "",
    "WORKLOAD" : "",
    "MAX_TXN_PER_PART" : "TXNS",
    "MAX_TXN_IN_FLIGHT" : "TIF",
    "PART_PER_TXN" : "PPT",
    "TUP_READ_PERC" : "TRD",
    "TUP_WRITE_PERC" : "TWR",
    "TXN_READ_PERC" : "RD",
    "TXN_WRITE_PERC" : "WR",
    "ZIPF_THETA" : "SKEW",
    "MSG_TIME_LIMIT" : "BT",
    "MSG_SIZE_MAX" : "BS",
    "DATA_PERC":"D",
    "ACCESS_PERC":"A",
    "PERC_PAYMENT":"PP",
    "MPR":"MPR",
    "REQ_PER_QUERY": "RPQ",
    "MODE":"",
    "PRIORITY":"",
    "ABORT_PENALTY":"PENALTY",
    "STRICT_PPT":"SPPT",
    "NETWORK_DELAY":"NDLY",
    "NETWORK_DELAY_TEST":"NDT",
    "REPLICA_CNT":"RN",
    "SYNTH_TABLE_SIZE":"TBL",
    "ISOLATION_LEVEL":"LVL",
    "YCSB_ABORT_MODE":"ABRTMODE",
    "NUM_WH":"WH",
}

fmt_title=["NODE_CNT","CC_ALG","ACCESS_PERC","TXN_WRITE_PERC","PERC_PAYMENT","MPR","MODE","MAX_TXN_IN_FLIGHT","SEND_THREAD_CNT","REM_THREAD_CNT","THREAD_CNT","TXN_WRITE_PERC","TUP_WRITE_PERC","ZIPF_THETA","NUM_WH"]

##############################
# PLOTS
##############################
def ycsb_once():
    wl = 'YCSB'
    nnodes = [2]
    algos=['HDCC']
    base_table_size=1048576*8
    txn_write_perc = [1]
    tup_write_perc = [0.2]
    load = [10000]
    tcnt = [16]
    skew = [0.9]
    fmt = ["WORKLOAD","CC_ALG","ZIPF_THETA","NODE_CNT","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","THREAD_CNT"]
    exp = [[wl,algo,sk,n,base_table_size*n,tup_wr_perc,txn_wr_perc,ld,thr] for thr,txn_wr_perc,tup_wr_perc,ld,n,sk,algo in itertools.product(tcnt,txn_write_perc,tup_write_perc,load,nnodes,skew,algos)]
    return fmt,exp

def ycsb_scaling():
    wl = 'YCSB'
    nnodes = [1,2,4,6,8,12]
    algos=['HDCC']
    base_table_size=1048576*8
    txn_write_perc = [1]
    tup_write_perc = [0.2]
    load = [10000]
    tcnt = [16]
    ctcnt = [4]
    scnt = [2]
    rcnt = [2]
    mpr = [0.2]
    prorate = [0]
    skew = [0.9]
    fmt = ["WORKLOAD","CC_ALG","NODE_CNT","SYNTH_TABLE_SIZE","MPR","PRORATE_RATIO","TUP_WRITE_PERC","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","ZIPF_THETA","THREAD_CNT","CLIENT_THREAD_CNT","SEND_THREAD_CNT","REM_THREAD_CNT","CLIENT_SEND_THREAD_CNT","CLIENT_REM_THREAD_CNT"]
    exp = [[wl,algo,n,base_table_size*n,mpr,prorate_rate,tup_wr_perc,txn_wr_perc,ld,sk,thr,cthr,sthr,rthr,sthr,rthr] for thr,cthr,sthr,rthr,txn_wr_perc,tup_wr_perc,sk,ld,mpr,prorate_rate,n,algo in itertools.product(tcnt,ctcnt,scnt,rcnt,txn_write_perc,tup_write_perc,skew,load,mpr,prorate,nnodes,algos)]
    return fmt,exp

def ycsb_skew():
    wl = 'YCSB'
    nnodes = [2]
    algos=['HDCC']
    base_table_size=1048576*8
    txn_write_perc = [1]
    tup_write_perc = [0.2]
    load = [10000]
    tcnt = [16]
    skew = [0.1,0.3,0.5,0.7,0.9,1.1,1.3,1.5]
    fmt = ["WORKLOAD","CC_ALG","ZIPF_THETA","NODE_CNT","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","THREAD_CNT"]
    exp = [[wl,algo,sk,n,base_table_size*n,tup_wr_perc,txn_wr_perc,ld,thr] for thr,txn_wr_perc,tup_wr_perc,ld,n,sk,algo in itertools.product(tcnt,txn_write_perc,tup_write_perc,load,nnodes,skew,algos)]
    return fmt,exp

def ycsb_writes():
    wl = 'YCSB'
    nnodes = [2]
    algos=['HDCC', 'CALVIN']
    base_table_size=1048576*8
    txn_write_perc = [1.0]
    tup_write_perc = [0.0,0.2,0.4,0.6,0.8,1.0]
    load = [10000]
    tcnt = [16]
    skew = [0.9]
    fmt = ["WORKLOAD","CC_ALG","TUP_WRITE_PERC","NODE_CNT","SYNTH_TABLE_SIZE","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","ZIPF_THETA","THREAD_CNT"]
    exp = [[wl,algo,tup_wr_perc,n,base_table_size*n,txn_wr_perc,ld,sk,thr] for thr,txn_wr_perc,tup_wr_perc,ld,n,sk,algo in itertools.product(tcnt,txn_write_perc,tup_write_perc,load,nnodes,skew,algos)]
    return fmt,exp

def ycsb_dist_ratio():
    wl = 'YCSB'
    nnodes = [2]
    algos=['HDCC']
    base_table_size=1048576*8
    txn_write_perc = [1.0]
    tup_write_perc = [0.2]
    load = [10000]
    mpr=[0,0.2,0.4,0.6,0.8,1]
    fmt = ["WORKLOAD","CC_ALG","MPR","NODE_CNT","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT"]
    exp = [[wl,algo,mpr,n,base_table_size*n,tup_wr_perc,txn_wr_perc,ld] for ld,tup_wr_perc,txn_wr_perc,n,mpr,algo in itertools.product(load,tup_write_perc,txn_write_perc,nnodes,mpr,algos)]
    return fmt,exp

def ycsb_log():
    wl = 'YCSB'
    nnodes = [2]
    algos=['HDCC','CALVIN','SILO']
    base_table_size=1048576*8
    txn_write_perc = [1.0]
    tup_write_perc = [0.2]
    load = [10000]
    tcnt = [16]
    skew = [0.9]
    logging = ['true','false']
    fmt = ["WORKLOAD","CC_ALG","LOGGING","NODE_CNT","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","ZIPF_THETA","THREAD_CNT"]
    exp = [[wl,algo,log,n,base_table_size*n,tup_wr_perc,txn_wr_perc,ld,sk,thr] for thr,txn_wr_perc,tup_wr_perc,ld,n,sk,log,algo in itertools.product(tcnt,txn_write_perc,tup_write_perc,load,nnodes,skew,logging,algos)]
    return fmt,exp

def tpcc_scaling():
    wl = 'TPCC'
    nnodes = [1,2,4,6,8,12]
    algos=['HDCC']
    npercpay=[0.489]
    num_wh=[32]
    load = [10000]
    tcnt = [16]
    ctcnt = [4]
    prorate = [0]
    mpr = [0.15]
    mpr_neworder = [0.1]
    fmt = ["WORKLOAD","CC_ALG","NODE_CNT","PERC_PAYMENT","PRORATE_RATIO","NUM_WH","MAX_TXN_IN_FLIGHT","THREAD_CNT","CLIENT_THREAD_CNT","MPR","MPR_NEWORDER"]
    exp = [[wl,algo,n,pp,prorate_rate,wh*n,tif,thr,cthr,m,mn] for thr,cthr,tif,pp,prorate_rate,n,m,mn,wh,algo in itertools.product(tcnt,ctcnt,load,npercpay,prorate,nnodes,mpr,mpr_neworder,num_wh,algos)]
    return fmt,exp

def tpcc_wh():
    wl = 'TPCC'
    nnodes = [2]
    algos=['HDCC']
    npercpay=[0.489]
    num_wh=[216,128,64,32,16,8]
    load = [10000]
    tcnt = [16]
    ctcnt = [4]
    prorate = [0]
    mpr = [0.15]
    mpr_neworder = [0.1]
    fmt = ["WORKLOAD","CC_ALG","NUM_WH","NODE_CNT","PERC_PAYMENT","PRORATE_RATIO","MAX_TXN_IN_FLIGHT","THREAD_CNT","CLIENT_THREAD_CNT","MPR","MPR_NEWORDER"]
    exp = [[wl,algo,wh*n,n,pp,prorate_rate,tif,thr,cthr,m,mn] for thr,cthr,tif,pp,prorate_rate,n,m,mn,wh,algo in itertools.product(tcnt,ctcnt,load,npercpay,prorate,nnodes,mpr,mpr_neworder,num_wh,algos)]
    return fmt,exp

def tpcc_dist_ratio():
    wl = 'TPCC'
    algos=['HDCC']
    mpr=[0,0.2,0.4,0.6,0.8,1]
    nnodes = [2]
    npercpay=[0.489]
    wh=32
    load = [10000]
    fmt = ["WORKLOAD","CC_ALG","MPR","NODE_CNT","PERC_PAYMENT","NUM_WH","MAX_TXN_IN_FLIGHT"]
    exp = [[wl,algo,mpr,n,pp,wh*n,tif] for tif,pp,n,mpr,algo in itertools.product(load,npercpay,nnodes,mpr,algos)]
    return fmt,exp

##############################
# END PLOTS
##############################

experiment_map = {
    'ycsb_once': ycsb_once,
    'ycsb_scaling': ycsb_scaling,
    'ycsb_writes': ycsb_writes,
    'ycsb_skew': ycsb_skew,
    'ycsb_dist_ratio': ycsb_dist_ratio,
    'ycsb_log': ycsb_log,
    'tpcc_scaling': tpcc_scaling,
    'tpcc_wh': tpcc_wh,
    'tpcc_dist_ratio': tpcc_dist_ratio,
}


# Default values for variable configurations
configs = {
    "NODE_CNT" : 2,
    "THREAD_CNT": 16,
    "REPLICA_CNT": 0,
    "REPLICA_TYPE": "AP",
    "REM_THREAD_CNT": 2,
    "SEND_THREAD_CNT": 2,
    "CLIENT_NODE_CNT" : "NODE_CNT",
    "CLIENT_THREAD_CNT" : 4,
    "CLIENT_REM_THREAD_CNT" : 2,
    "CLIENT_SEND_THREAD_CNT" : 2,
    "MAX_TXN_PER_PART" : 500000,
    "WORKLOAD" : "YCSB",
    "CC_ALG" : "WAIT_DIE",
    "MPR" : 0.2,
    "TPORT_TYPE":"TCP",
    "TPORT_PORT":"18000",
    "PART_CNT": "NODE_CNT",
    "PART_PER_TXN": 2,
    "MAX_TXN_IN_FLIGHT": 10000,
    "NETWORK_DELAY": '0UL',
    "NETWORK_DELAY_TEST": 'false',
    "DONE_TIMER": "1 * 60 * BILLION // ~1 minutes",
    "WARMUP_TIMER": "1 * 60 * BILLION // ~1 minutes",
    "SEQ_BATCH_TIMER": "5 * 1 * MILLION // ~5ms -- same as CALVIN paper",
    "BATCH_TIMER" : "0",
    "PROG_TIMER" : "10 * BILLION // in s",
    "NETWORK_TEST" : "false",
    "ABORT_PENALTY": "10 * 1000000UL   // in ns.",
    "ABORT_PENALTY_MAX": "5 * 100 * 1000000UL   // in ns.",
    "MSG_TIME_LIMIT": "0",
    "MSG_SIZE_MAX": 4096,
    "TXN_WRITE_PERC":1.0,
    "PRIORITY":"PRIORITY_ACTIVE",
    "TWOPL_LITE":"false",
#YCSB
    "INIT_PARALLELISM" : 8,
    "TUP_WRITE_PERC":0.2,
    "ZIPF_THETA":0.9,
    "ACCESS_PERC":0.03,
    "DATA_PERC": 100,
    "REQ_PER_QUERY": 10,
    "SYNTH_TABLE_SIZE":"1048576*8",
#TPCC
    "NUM_WH":32,
    "PERC_PAYMENT":0.489,
    "MPR_NEWORDER":"MPR",
#TXN
    "PRORATE_RATIO":0,
    "LOGGING":"false",
#OTHERS
    "DEBUG_DISTR":"false",
    "DEBUG_ALLOC":"false",
    "DEBUG_RACE":"false",
    "MODE":"NORMAL_MODE",
    "SHMEM_ENV":"false",
    "STRICT_PPT":0,
    "SET_AFFINITY":"true",
    "SERVER_GENERATE_QUERIES":"false",
    "SKEW_METHOD":"ZIPF",
    "ENVIRONMENT_EC2":"false",
    "YCSB_ABORT_MODE":"false",
    "LOAD_METHOD": "LOAD_MAX",
    "ISOLATION_LEVEL":"SERIALIZABLE"
}

