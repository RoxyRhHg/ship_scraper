"""
scraper 实时监控 — 轮询 state 文件, 检测低产和卡死
用法: python monitor_scraper.py [--interval 30]
条件:
  1. 连续 2 次检查新增 <5  → 低产告警
  2. 超 300s 无新增        → 卡死告警
"""
import json
import time
import sys
from pathlib import Path

STATE_PATH = Path(r"e:\guangdianbishe\ship_scraper_v10\output\state\scraper_state.json")
INTERVAL = int(sys.argv[1]) if len(sys.argv) > 1 else 30

print(f"[Monitor] Watching {STATE_PATH}")
print(f"[Monitor] Interval: {INTERVAL}s")
print(f"[Monitor] Rules: 2x low-yield (<5) → ALERT | 300s no-progress → STALL\n")

prev_count = None
prev_time = None
low_streak = 0
stall_alerted = False

while True:
    try:
        with open(STATE_PATH) as f:
            state = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        print(f"[{time.strftime('%H:%M:%S')}] 等待 state 文件...")
        time.sleep(INTERVAL)
        continue

    curr_count = state.get("accepted_count", 0)
    last_save = state.get("last_save", 0)
    rejections = state.get("rejection_counts", {})
    now = time.time()

    if prev_count is not None:
        new = curr_count - prev_count
        elapsed = now - prev_time
        ts = time.strftime("%H:%M:%S")

        print(f"[{ts}] accepted={curr_count} (+{new})  elapsed={elapsed:.0f}s  "
              f"top_reject={sorted(rejections.items(), key=lambda x: -x[1])[:3]}")

        # 检测 1: 连续低产
        if 0 < new < 5:
            low_streak += 1
            if low_streak >= 2:
                print(f"\n  ⚠️ 连续 {low_streak} 次低产 (+{new})！诊断中...\n")
                # 分析 rejection 分布
                dl_failed = rejections.get("download_failed", 0)
                other = sum(v for k, v in rejections.items() if k != "download_failed")
                print(f"  Rejection 分析: download_failed={dl_failed}, other={other}")
                if dl_failed > other * 2:
                    print(f"  → 诊断: download_failed 占比过高 ({dl_failed}/{dl_failed+other})")
                    print(f"  → 建议: 代理源不稳定, 降低 --proxy-workers 或切换到直连源")
                else:
                    print(f"  → 诊断: 候选质量下降, 非网络问题")
                print()
                low_streak = 0  # 重置, 下次再触发
        else:
            low_streak = max(0, low_streak - 1)  # 有进展就减计数

        # 检测 2: 卡死
        if new == 0 and elapsed > 300 and not stall_alerted:
            print(f"\n  🔴 STALL! {elapsed:.0f}s 无进展！")
            print(f"  → 线程可能 hang 在 iter_content() 中")
            print(f"  → 300s 全局超时应该已触发, 检查终端是否有 [Stall] 打印")
            print(f"  → 如果持续卡死, 终止进程重新运行:")
            print(f"     python scraper_v10.py --no-proxy --sources maritimequest --target 5000")
            print()
            stall_alerted = True
        elif new > 0:
            stall_alerted = False

    else:
        print(f"[{time.strftime('%H:%M:%S')}] 初始: accepted={curr_count}")

    prev_count = curr_count
    prev_time = now
    time.sleep(INTERVAL)
