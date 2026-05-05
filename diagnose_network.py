"""
网络链路诊断 v2 — 使用 scraper 源码中真实的 URL
运行方式:
  python diagnose_network.py --proxy http://127.0.0.1:7890
  python diagnose_network.py --no-proxy       # 测试全部直连

诊断逻辑:
  - 阶段 1: 测试 HTML 索引页连通性（跟 scraper 第一步完全一样）
  - 阶段 2: 测试真实图片下载（模拟 iter_content 卡死场景）
  - 阶段 3: 并发下载压力（模拟多线程同时跑）
  - 最后给出分源建议
"""
import argparse
import time
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

# ═══════════════════════════════════════════════════════════════════════════
# 这些全部来自 scraper_v10.py 源码，保证和实际爬虫走完全相同的 URL
# ═══════════════════════════════════════════════════════════════════════════

# NavSource 索引页 — 注意是 HTTP 不是 HTTPS！
NAVSOURCE_TEST_URLS = [
    ("NavSource-DD",   "http://www.navsource.org/archives/05/0502.htm"),
    ("NavSource-DDG",  "http://www.navsource.org/archives/05/0504.htm"),
    ("NavSource-CG",   "http://www.navsource.org/archives/04/0402.htm"),
    ("NavSource-CV",   "http://www.navsource.org/archives/02/0202.htm"),
]

# MaritimeQuest 索引页 — 部分 HTTP 部分 HTTPS
MQ_TEST_URLS = [
    ("MQ-DD-NOS",  "https://www.maritimequest.com/warship_directory/us_navy_pages/destroyers/us_navy_destroyer_hull_number_index.htm"),
    ("MQ-DDG",     "https://www.maritimequest.com/warship_directory/us_navy_pages/destroyers/us_navy_ddg_hull_number_index.htm"),
    ("MQ-Cruiser", "https://www.maritimequest.com/warship_directory/us_navy_pages/cruisers/us_navy_cruiser_hull_number_index.htm"),
    ("MQ-Carrier", "http://mail.maritimequest.com/warship_directory/us_navy_pages/aircraft_carriers/us_aircraft_carrier_index.htm"),
]

# 代理源
PROXY_TEST_URLS = [
    ("Bing",       "https://www.bing.com/images/search?q=uss+arleigh+burke+ddg+51+destroyer+photo&first=1"),
    ("Wikimedia",  "https://commons.wikimedia.org/wiki/Category:Destroyers_of_the_United_States_Navy"),
]


def test_http(url: str, proxy: str = "", timeout: int = 15) -> dict:
    """单次 HTTP GET，返回状态和耗时"""
    proxies = {"http": proxy, "https": proxy} if proxy else None
    result = {"url": url, "status": "unknown", "elapsed": 0, "size_kb": 0, "error": ""}
    t0 = time.time()
    try:
        resp = requests.get(url, proxies=proxies, timeout=timeout, stream=True)
        # 只读前面一点确认响应有效
        body = resp.content[:4096]
        resp.close()
        result["status"] = f"HTTP {resp.status_code}"
        result["elapsed"] = round(time.time() - t0, 2)
        result["size_kb"] = round(len(body) / 1024, 1)
    except requests.exceptions.ConnectTimeout:
        result["status"] = "CONNECT_TIMEOUT"
        result["error"] = f"TCP 握手超时 ({timeout}s) → 端口不通或被墙"
    except requests.exceptions.ReadTimeout:
        result["status"] = "READ_TIMEOUT"
        result["error"] = f"读超时 ({timeout}s) → 通了但极慢，图下载会卡死"
    except requests.exceptions.ConnectionError as e:
        msg = str(e)[:120]
        if "Connection refused" in msg or "Connection reset" in msg or "RemoteDisconnected" in msg:
            result["status"] = "CONN_RESET"
            result["error"] = f"连接被拒/重置 → GFW 阻断或站点屏蔽"
        else:
            result["status"] = "CONN_ERROR"
            result["error"] = msg
    except Exception as e:
        result["status"] = "ERROR"
        result["error"] = f"{type(e).__name__}: {str(e)[:120]}"
    return result


def test_image_download(label: str, url: str, proxy: str = "", timeout: int = 30) -> dict:
    """模拟 _download_and_prefilter 的真实下载行为"""
    proxies = {"http": proxy, "https": proxy} if proxy else None
    result = {"label": label, "url": url[:90], "status": "unknown", "size_kb": 0, "elapsed": 0, "error": ""}
    t0 = time.time()
    try:
        headers = {"Referer": url, "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
        resp = requests.get(url, proxies=proxies, timeout=(10, timeout), stream=True, headers=headers)
        resp.raise_for_status()
        total = 0
        for chunk in resp.iter_content(chunk_size=64 * 1024):
            if not chunk:
                continue
            total += len(chunk)
            if total > 1 * 1024 * 1024:  # 取 1MB 测速
                break
        result["status"] = "OK"
        result["size_kb"] = round(total / 1024, 1)
        result["elapsed"] = round(time.time() - t0, 2)
        result["speed_kbps"] = round(result["size_kb"] / result["elapsed"]) if result["elapsed"] > 0 else 0
    except requests.exceptions.ReadTimeout:
        result["status"] = "READ_TIMEOUT"
        result["error"] = "iter_content 中卡死 — 这是 Stall 的根因！"
    except requests.exceptions.ConnectTimeout:
        result["status"] = "CONNECT_TIMEOUT"
        result["error"] = "TCP 连接超时"
    except Exception as e:
        result["status"] = "FAILED"
        result["error"] = f"{type(e).__name__}: {str(e)[:120]}"
    return result


def run_stage(label: str, urls: list, proxy: str, timeout: int) -> list:
    """跑一组测试，打印结果"""
    results = []
    for name, url in urls:
        r = test_http(url, proxy=proxy, timeout=timeout)
        r["name"] = name
        results.append(r)
        ok = "200" in r["status"]
        icon = "+" if ok else "!"
        print(f"  [{icon}] {r['elapsed']:5.1f}s  {r['status']:<22s} {name:<20s} {url[:70]}")
        if r["error"]:
            print(f"       ↳ {r['error']}")
    return results


def main():
    parser = argparse.ArgumentParser(description="链路诊断 v2")
    parser.add_argument("--proxy", default="http://127.0.0.1:7890")
    parser.add_argument("--no-proxy", action="store_true")
    args = parser.parse_args()
    proxy = "" if args.no_proxy else args.proxy

    print("=" * 75)
    print("  链路诊断 v2 — 使用 scraper 源码真实 URL")
    print(f"  代理: {'直连 (无代理)' if not proxy else proxy}")
    print("=" * 75)

    # ─── 阶段 1: HTML 索引页 ───────────────────────────────────────
    print(f"\n{'─' * 70}")
    print("阶段 1: HTML 索引页连通性（直连源）")
    print("{:─^70}".format("─"))
    print()

    print("  [NavSource — HTTP 直连]")
    ns_results = run_stage("NavSource", NAVSOURCE_TEST_URLS, proxy="", timeout=15)

    print("\n  [MaritimeQuest — 直连]")
    mq_results = run_stage("MaritimeQuest", MQ_TEST_URLS, proxy="", timeout=15)

    print(f"\n  [代理源 — 走 {proxy}]")
    proxy_results = run_stage("Proxy", PROXY_TEST_URLS, proxy=proxy, timeout=15)

    # ─── 阶段 2: 图片下载 ─────────────────────────────────────────
    print(f"\n{'─' * 70}")
    print("阶段 2: 真实图片下载（模拟 iter_content）")
    print("{:─^70}".format("─"))

    # 从 MQ 索引页里抓一张真实图片 URL 来测
    print("\n  正在从 MQ 索引页抓取真实图片 URL...")
    real_image_url = None
    for name, url in MQ_TEST_URLS:
        try:
            resp = requests.get(url, timeout=15)
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(resp.text, "html.parser")
            for img in soup.find_all("img"):
                src = img.get("src", "")
                if src and ("photo" in src.lower() or "image" in src.lower() or "jpg" in src.lower()):
                    from urllib.parse import urljoin
                    real_image_url = urljoin(url, src)
                    if "maritimequest.com" in real_image_url:
                        break
            if real_image_url:
                break
        except Exception:
            continue

    image_tests = []

    # 测试 1: MQ 直连下载
    if real_image_url:
        print(f"  测试直连图片: {real_image_url[:80]}...")
        r = test_image_download("MQ直连图", real_image_url, proxy="", timeout=30)
        image_tests.append(("直连", r))
        icon = "+" if r["status"] == "OK" else "!"
        print(f"  [{icon}] {r['elapsed']:5.1f}s  {r['status']:<15s} {r['size_kb']:6.1f}KB  speed={r.get('speed_kbps', '?'):}KB/s")
        if r["error"]:
            print(f"       ↳ {r['error']}")
    else:
        print("  [!] 无法从 MQ 抓取图片 URL，跳过直连图片测试")

    # 测试 2: Bing 直接搜一张图（走代理）
    print(f"\n  测试代理图片: Bing 搜索图片...")
    try:
        # Bing 图片搜索 + 取第一个结果
        resp = requests.get(
            "https://www.bing.com/images/search?q=uss+arleigh+burke+ddg+51&first=1",
            proxies={"http": proxy, "https": proxy} if proxy else None,
            timeout=15,
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
        )
        # 从 Bing 页面提取 murl
        import re
        murls = re.findall(r'"murl"\s*:\s*"([^"]+)"', resp.text)
        if murls:
            bing_img = murls[0]
            print(f"  测试代理图片: {bing_img[:80]}...")
            r = test_image_download("Bing代理图", bing_img, proxy=proxy, timeout=30)
            image_tests.append(("代理", r))
            icon = "+" if r["status"] == "OK" else "!"
            print(f"  [{icon}] {r['elapsed']:5.1f}s  {r['status']:<15s} {r['size_kb']:6.1f}KB  speed={r.get('speed_kbps', '?'):}KB/s")
            if r["error"]:
                print(f"       ↳ {r['error']}")
        else:
            print("  [!] Bing 未返回图片结果（可能验证页面）")
    except Exception as e:
        print(f"  [!] Bing 搜索失败: {e}")

    # ─── 阶段 3: 并发压力 ─────────────────────────────────────────
    print(f"\n{'─' * 70}")
    print("阶段 3: 并发压力测试")
    print("{:─^70}".format("─"))

    if real_image_url:
        print(f"\n  MQ 直连并发 x5: {real_image_url[:60]}...")
        results = []
        with ThreadPoolExecutor(max_workers=5) as ex:
            futures = {ex.submit(test_image_download, f"worker-{i}", real_image_url, "", 30): i for i in range(5)}
            for f in as_completed(futures, timeout=90):
                try:
                    results.append(f.result(timeout=10))
                except Exception:
                    results.append({"status": "FUTURE_FAILED", "elapsed": 0, "size_kb": 0})

        ok = [r for r in results if r["status"] == "OK"]
        fail = [r for r in results if r["status"] != "OK"]
        times = [r["elapsed"] for r in ok]
        print(f"  成功: {len(ok)}/5 | 失败: {len(fail)}/5")
        if times:
            print(f"  耗时: min={min(times):.1f}s max={max(times):.1f}s avg={sum(times)/len(times):.1f}s")
    else:
        print("  跳过（无可用图片 URL）")
        ok, fail = [], []

    # ─── 诊断结论 ──────────────────────────────────────────────────
    print(f"\n{'=' * 75}")
    print("  诊断结论")
    print("=" * 75)

    ns_ok = sum(1 for r in ns_results if "200" in r["status"])
    mq_ok = sum(1 for r in mq_results if "200" in r["status"])
    proxy_ok = sum(1 for r in proxy_results if "200" in r["status"])

    print(f"""
  NavSource 直连: {ns_ok}/{len(ns_results)} 通  {'✓ 可用' if ns_ok >= 1 else '✗ 不可用'}
  MaritimeQ 直连: {mq_ok}/{len(mq_results)} 通  {'✓ 可用' if mq_ok >= 1 else '✗ 不可用'}
  代理源:         {proxy_ok}/{len(proxy_results)} 通  {'✓ 可用' if proxy_ok >= 1 else '✗ 不可用'}
""")

    # 给出运行建议
    print("  ▶ 建议运行命令:")
    if ns_ok >= 1 and mq_ok >= 1:
        print("    直连源可用 → 可以用 --no-proxy 只跑直连源")
        print("    python scraper_v10.py --no-proxy --sources maritimequest,navsource")
    elif proxy_ok >= 1:
        print("    代理源可用 → 所有源都走代理")
        print("    python scraper_v10.py --proxy http://127.0.0.1:7890 --proxy-workers 2 --batch-size 30")
    else:
        print("    [!] 直连和代理都不通，请先检查网络/Clash")

    if proxy_ok >= 1:
        print(f"\n  ▶ Clash 配置自检清单:")
        print(f"    [ ] TUN Mode: 必须 OFF（会干扰显式代理）")
        print(f"    [ ] System Proxy: 建议 OFF（代码已写死代理）")
        print(f"    [ ] 模式: 建议 Global（不用 Rule）")
        print(f"    [ ] 节点: 手动固定一个稳的，不开自动切换")
        print(f"    [ ] 端口: Mixed Port = 7890")
        print(f"    [ ] 只保留 HTTP 代理，关闭 SOCKS5")


if __name__ == "__main__":
    main()
