"""
Ship Hull Number Image Scraper v10
===================================
v10 架构重构版 — 融合 v2 稳定性 + v6 多源广度:

核心改进 (针对 v6 stall + v2 数量不足):
- 独立 Session 池: 每 worker 独享 Session, 消除连接池竞争
- 分源并发: 直连源 (MQ/NavSource) 高并发, 代理源 (Bing/USNI/Wiki) 低并发
- URL canonicalization: thumbnail→full-size, 去除查询参数, 减少重复
- 同页限制: 同一 page_url 最多接受 3 张图
- easyocr batch inference: GPU 批量处理 4-8 张/次
- 逐 future 超时 (30s) 替代 wait(60s) 批次等待
- 指数退避重试: 1s→2s→4s→8s→16s, 最多 5 次
- Bing 搜索翻 5 页 (原 3 页)
- 代理锁定: 启动时探测一次, 不复探测

用法:
  python scraper_v10.py                                           # 默认运行
  python scraper_v10.py --target 5000 --time-limit 28800          # 8小时 5000 目标
  python scraper_v10.py --proxy http://127.0.0.1:7890             # 指定代理
  python scraper_v10.py --no-proxy                                # 全部直连
  python scraper_v10.py --sources bing,maritimequest,navsource    # 指定源
  python scraper_v10.py --target 200 --time-limit 900             # 15min 验证
"""

from __future__ import annotations

import csv
import hashlib
import imghdr
import json
import os
import random
import re
import shutil
import sys
import time
import threading
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed, Future, wait
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from threading import Lock
from typing import Optional
from urllib.parse import urljoin, urlparse, quote, urlunparse

import socket
import warnings
import requests
from bs4 import BeautifulSoup

warnings.filterwarnings("ignore", message="Palette images with Transparency")
socket.setdefaulttimeout(30)

_BS4_PARSER = "lxml"
try:
    BeautifulSoup("", "lxml")
except Exception:
    _BS4_PARSER = "html.parser"


# ═══════════════════════════════════════════════════════════════════════
# 代理探测 (启动时调用一次, 不复探测)
# ═══════════════════════════════════════════════════════════════════════

PROXY_CANDIDATE_PORTS = (7890, 7891, 1080, 10808, 10809, 8118, 8888, 8080)


def _probe_proxy(proxy_url: str, timeout: float = 3.0) -> bool:
    try:
        resp = requests.get("https://www.bing.com", proxies={"http": proxy_url, "https": proxy_url},
                            timeout=timeout, stream=True)
        resp.close()
        return resp.status_code < 500
    except Exception:
        return False


def auto_detect_proxy() -> str:
    for var in ("HTTPS_PROXY", "https_proxy", "HTTP_PROXY", "http_proxy", "ALL_PROXY", "all_proxy"):
        val = os.environ.get(var, "").strip()
        if val:
            print(f"[Proxy] Detected from env {var}: {val}")
            return val
    for port in PROXY_CANDIDATE_PORTS:
        for scheme in ("http", "socks5"):
            url = f"{scheme}://127.0.0.1:{port}"
            if _probe_proxy(url):
                print(f"[Proxy] Auto-detected: {url}")
                return url
    print("[Proxy] No proxy detected.")
    return ""


# ═══════════════════════════════════════════════════════════════════════
# 配置
# ═══════════════════════════════════════════════════════════════════════

@dataclass
class Config:
    output_dir: Path = Path(r"E:\guangdianbishe\ship_scraper_v10\output")
    target: int = 5000
    direct_workers: int = 6           # 直连并发 (MQ, NavSource)
    proxy_workers: int = 4            # 代理并发 (Bing, USNI, Wiki)
    min_width: int = 500
    min_height: int = 350
    target_width: int = 1024
    target_height: int = 768
    sharpness_threshold: float = 60.0
    min_ocr_digits: int = 1
    max_ocr_chars: int = 15
    request_timeout: int = 30
    min_file_size: int = 10 * 1024
    seed: int = 42
    sources: tuple[str, ...] = ("bing", "usni", "navsource", "maritimequest", "wikimedia")
    max_per_source: int = 10000
    time_limit: int = 28800            # 8小时, 0=无限制
    batch_size: int = 100
    review_interval: int = 100
    max_stall_retries: int = 5         # v10: 2→5
    proxy: str = "auto"
    proxy_domains: tuple[str, ...] = ("bing.com", "usni.org", "wikimedia.org", "photos.usni.org")
    gpu_ocr: bool = True
    discovery_workers: int = 4
    mq_fetch_workers: int = 6          # MQ Phase 2 并行度 (直连)
    ocr_batch_size: int = 6            # easyocr batch inference
    per_page_limit: int = 3            # 同一 page_url 最多接受几张


# ═══════════════════════════════════════════════════════════════════════
# 弦号检测
# ═══════════════════════════════════════════════════════════════════════

HULL_NUMBER_PATTERN = re.compile(
    r'\b(?:DDG|DD|FFG|FF|CG|CVN|CVA|CV|LHD|LPD|LHA|LCS|SSN|SSBN|SSG|SS|PC|WHEC|WMEC|WPB|PB)'
    r'[-\s]?\d{1,4}\b',
    re.IGNORECASE
)

HULL_NUMBER_LOOSE = re.compile(
    r'\b\d{2,4}\b.*(?:destroyer|frigate|cruiser|carrier|submarine|amphibious|corvette|patrol)',
    re.IGNORECASE
)


def title_has_hull_number(title: str) -> bool:
    return bool(HULL_NUMBER_PATTERN.search(title)) or bool(HULL_NUMBER_LOOSE.search(title))


def url_has_hull_number(url: str) -> bool:
    return bool(HULL_NUMBER_PATTERN.search(url))


# ═══════════════════════════════════════════════════════════════════════
# URL 规范化 — v10 新增, 减少 thumbnail/fullsize 重复
# ═══════════════════════════════════════════════════════════════════════

def canonicalize_image_url(url: str) -> str:
    """Remove query params and normalize thumbnail URLs to reduce duplicates."""
    parsed = urlparse(url)
    # Strip query string and fragment for dedup purposes
    clean = urlunparse((parsed.scheme, parsed.netloc, parsed.path, "", "", ""))
    # MQ: remove thumbnail suffixes like _tn, _small, _thumb
    lower_path = clean.lower()
    for marker in ("_tn.", "-tn.", "_small.", "-small.", "_thumb.", "-thumb.",
                   "_thumbnail.", "-thumbnail."):
        if marker in lower_path:
            clean = clean.lower().replace(marker, ".")
            break
    return clean


# ═══════════════════════════════════════════════════════════════════════
# 图像工具
# ═══════════════════════════════════════════════════════════════════════

def image_sha1(path: Path) -> str:
    h = hashlib.sha1()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def image_dhash(path: Path, hash_size: int = 8) -> str:
    try:
        from PIL import Image
        img = Image.open(path).convert("L").resize((hash_size + 1, hash_size), Image.LANCZOS)
        pixels = list(img.getdata())
        bits = []
        for row in range(hash_size):
            for col in range(hash_size):
                bits.append("1" if pixels[row * (hash_size + 1) + col] < pixels[row * (hash_size + 1) + col + 1] else "0")
        return hashlib.md5("".join(bits).encode()).hexdigest()[:16]
    except Exception:
        return ""


def hamming_distance(h1: str, h2: str) -> int:
    if not h1 or not h2 or len(h1) != len(h2):
        return 999
    return sum(c1 != c2 for c1, c2 in zip(h1, h2))


def check_sharpness(path: Path) -> float:
    try:
        import cv2
        import numpy as np
        from PIL import Image
        img = np.array(Image.open(path).convert("L"))
        return float(cv2.Laplacian(img, cv2.CV_64F).var())
    except Exception:
        return 0.0


def _safe_convert_to_rgb(img):
    from PIL import Image
    if img.mode == "P" and "transparency" in img.info:
        return img.convert("RGBA").convert("RGB")
    return img.convert("RGB")


def normalize_to_jpeg(src: Path, dst: Path, tw: int, th: int) -> tuple[int, int]:
    from PIL import Image
    img = _safe_convert_to_rgb(Image.open(src))
    w, h = img.size
    scale = min(tw / w, th / h)
    new_w, new_h = int(w * scale), int(h * scale)
    img = img.resize((new_w, new_h), Image.LANCZOS)
    canvas = Image.new("RGB", (tw, th), (0, 0, 0))
    canvas.paste(img, ((tw - new_w) // 2, (th - new_h) // 2))
    canvas.save(dst, "JPEG", quality=92)
    return tw, th


def safe_unlink(path: Path):
    for _ in range(3):
        try:
            path.unlink(missing_ok=True)
            return
        except PermissionError:
            time.sleep(0.1)
    try:
        path.unlink(missing_ok=True)
    except Exception:
        pass


# ═══════════════════════════════════════════════════════════════════════
# OCR — v10: batch inference
# ═══════════════════════════════════════════════════════════════════════

_ocr_reader = None
_ocr_lock = threading.Lock()


def get_ocr_reader(gpu: bool = True):
    global _ocr_reader
    if _ocr_reader is None:
        with _ocr_lock:
            if _ocr_reader is None:
                import easyocr
                _ocr_reader = easyocr.Reader(["en"], gpu=gpu, verbose=False)
    return _ocr_reader


# ═══════════════════════════════════════════════════════════════════════
# 水印/垃圾文本过滤
# ═══════════════════════════════════════════════════════════════════════

WATERMARK_TEXT_PATTERNS = [
    r'www\.[a-z0-9\-]+\.(com|org|net|gov|mil|co\.[a-z]+)',
    r'[a-z0-9\-]+\.(com|org|net|gov|mil)',
    r'https?://\S+',
    r'©.*',
    r'\(c\).*',
    r'copyright.*',
    r'photo\s*(by|credit|courtesy).*',
    r'credit.*',
    r'source.*',
    r'(maritimequest|navsource|seaforces|usni|dvidshub|flickr|wikimedia)',
    r'(getty|shutterstock|alamy|reuters|ap\s+photo)',
    r'(photographer|photograph|image|picture)\s*(by|credit|courtesy)?',
    r'\d{5,}\.jpg',
    r'u\.s\.\s*naval\s*institute',
    r'usni\s*photo',
]
_WATERMARK_TEXT_RE = re.compile('|'.join(WATERMARK_TEXT_PATTERNS), re.IGNORECASE)

WATERMARK_DOMAINS = [
    "alamy.com", "alamy.", "shutterstock.com", "shutterstock.",
    "gettyimages.com", "getty.", "123rf.com", "123rf.",
    "dreamstime.com", "dreamstime.", "depositphotos.com",
    "istockphoto.com", "istock.", "bigstockphoto.com",
    "adobestock.com", "canstockphoto.com",
    "pinterest.com", "pinimg.com",
    "facebook.com", "fbcdn.net",
    "instagram.com", "cdninstagram.com",
    "twitter.com", "twimg.com",
    "tiktok.com", "youtube.com", "ytimg.com",
    "freepik.com", "vectorstock.com", "vecteezy.com",
]


def strip_watermark_text(text: str) -> str:
    tokens = text.split()
    cleaned = []
    for token in tokens:
        if _WATERMARK_TEXT_RE.search(token):
            continue
        if token.isdigit() and len(token) > 6:
            continue
        cleaned.append(token)
    return " ".join(cleaned)


def is_watermark_domain(url: str) -> bool:
    lower = url.lower()
    return any(d in lower for d in WATERMARK_DOMAINS)


def is_watermark_image_url(url: str) -> bool:
    lower = url.lower()
    bad_keywords = [
        "logo", "icon", "banner", "button", "flag", "avatar",
        "thumbnail", "thumb", "preview", "placeholder",
        "watermark", "wm.", "-wm-", "_wm_",
        "game", "model", "toy", "hobby", "3d", "render", "painting", "decal",
        "sprite", "stamp", "badge", "sticker",
        "map", "chart", "diagram", "illustration",
    ]
    return any(kw in lower for kw in bad_keywords)


# ═══════════════════════════════════════════════════════════════════════
# OCR batch inference — v10 新增
# ═══════════════════════════════════════════════════════════════════════

def batch_ocr_check(image_paths: list[Path], min_digits: int = 1, max_chars: int = 15,
                    gpu: bool = True) -> list[tuple[bool, str, float]]:
    """GPU batch OCR: 一次处理多张图片, 效率 3-4x 于单张处理."""
    if not image_paths:
        return []

    results = []
    try:
        reader = get_ocr_reader(gpu=gpu)
        # easyocr 接受 list of image paths, 内部 batch 处理
        raw_results = reader.readtext([str(p) for p in image_paths], detail=1, paragraph=False)
    except Exception:
        return [(False, "", 0.0)] * len(image_paths)

    # readtext 返回的是 list[list[(bbox, text, conf)]]
    for img_results in raw_results:
        if img_results is None:
            results.append((False, "", 0.0))
            continue
        all_text = ""
        max_conf = 0.0
        for _bbox, text, conf in img_results:
            all_text += text + " "
            max_conf = max(max_conf, conf)
        cleaned = strip_watermark_text(all_text.strip())
        digit_count = sum(1 for c in cleaned if c.isdigit())
        alnum_count = sum(1 for c in cleaned if c.isalnum())
        if digit_count < min_digits or alnum_count > max_chars:
            results.append((False, cleaned, max_conf))
        else:
            results.append((True, cleaned, max_conf))

    return results


# ═══════════════════════════════════════════════════════════════════════
# 数据模型
# ═══════════════════════════════════════════════════════════════════════

@dataclass
class CandidateImage:
    source: str
    page_url: str
    image_url: str
    source_id: str
    title: str = ""
    has_hull_in_title: bool = False


# ═══════════════════════════════════════════════════════════════════════
# 跨运行去重管理器 (同 v6, 增加 canonical URL)
# ═══════════════════════════════════════════════════════════════════════

class DedupManager:
    ACCEPTED_CSV_NAMES = {"accepted.csv", "accepted_china.csv"}

    def __init__(self, workspace_root: Path, current_output: Path):
        self.image_urls: set[str] = set()
        self.canonical_urls: set[str] = set()    # v10: canonical URL set
        self.page_urls: set[str] = set()
        self.sha1_set: set[str] = set()
        self._workspace = workspace_root
        self._current = current_output
        self._load_all()

    def _load_all(self):
        print("[Dedup] Building global dedup index...")
        self._scan_accepted_csvs()
        self._scan_processed_images()
        current_csv = self._current / "reports" / "accepted.csv"
        if current_csv.exists():
            self._load_csv(current_csv)
        print(f"[Dedup] Index ready: {len(self.image_urls)} image URLs, "
              f"{len(self.page_urls)} page URLs, {len(self.sha1_set)} SHA1")

    def _load_csv(self, csv_path: Path):
        try:
            with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
                for row in csv.DictReader(f):
                    img = (row.get("image_url") or "").strip()
                    pg = (row.get("page_url") or "").strip()
                    sha = (row.get("sha1") or "").strip()
                    if img:
                        self.image_urls.add(img)
                        self.canonical_urls.add(canonicalize_image_url(img))
                    if pg:
                        self.page_urls.add(pg)
                    if sha:
                        self.sha1_set.add(sha)
        except Exception:
            pass

    def _scan_accepted_csvs(self):
        count = 0
        for csv_path in self._workspace.rglob("*.csv"):
            try:
                csv_path.resolve().relative_to(self._current.resolve())
                continue
            except ValueError:
                pass
            if csv_path.name not in self.ACCEPTED_CSV_NAMES:
                continue
            self._load_csv(csv_path)
            count += 1
        print(f"  [Dedup] Scanned {count} external accepted CSV files")

    def _scan_processed_images(self):
        processed_dirs = []
        for d in self._workspace.iterdir():
            if not d.is_dir():
                continue
            try:
                if d.resolve() == self._current.resolve():
                    continue
            except RuntimeError:
                pass
            for sub in ["processed", "train/images", "test/images",
                        "labeling/seed/images", "labeling/hard/images"]:
                p = d / sub
                if p.is_dir():
                    processed_dirs.append(p)
        img_count = 0
        for proc_dir in processed_dirs:
            for ext in ("*.jpg", "*.png"):
                for img_path in proc_dir.glob(ext):
                    try:
                        sha1 = image_sha1(img_path)
                        if sha1:
                            self.sha1_set.add(sha1)
                            img_count += 1
                    except Exception:
                        continue
        print(f"  [Dedup] Scanned {img_count} processed images, {len(self.sha1_set)} total SHA1")

    def is_duplicate(self, image_url: str, page_url: str = "") -> bool:
        if image_url in self.image_urls:
            return True
        if canonicalize_image_url(image_url) in self.canonical_urls:
            return True
        if page_url and page_url in self.page_urls:
            return True
        return False

    def is_sha1_dup(self, sha1_val: str) -> bool:
        return sha1_val in self.sha1_set

    def add(self, image_url: str, page_url: str = "", sha1_val: str = ""):
        if image_url:
            self.image_urls.add(image_url)
            self.canonical_urls.add(canonicalize_image_url(image_url))
        if page_url:
            self.page_urls.add(page_url)
        if sha1_val:
            self.sha1_set.add(sha1_val)


# ═══════════════════════════════════════════════════════════════════════
# Session 池 — v10 核心改进: 每 worker 独立 Session
# ═══════════════════════════════════════════════════════════════════════

class SessionPool:
    """每 worker 一个独立 Session, 消除连接池竞态."""

    def __init__(self, proxy_url: str = "", pool_size: int = 8):
        self.proxy_url = proxy_url
        self._sessions: list[requests.Session] = []
        self._lock = threading.Lock()
        self._idx = 0
        for _ in range(pool_size):
            self._sessions.append(self._make_session())

    def _make_session(self) -> requests.Session:
        s = requests.Session()
        s.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "image/*,*/*;q=0.8",
        })
        if self.proxy_url:
            s.proxies = {"http": self.proxy_url, "https": self.proxy_url}
        # 小连接池, 避免代理压力
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=2, pool_maxsize=4, max_retries=1,
        )
        s.mount("https://", adapter)
        s.mount("http://", adapter)
        return s

    def get(self) -> requests.Session:
        with self._lock:
            s = self._sessions[self._idx % len(self._sessions)]
            self._idx += 1
            return s

    def close_all(self):
        for s in self._sessions:
            try:
                s.close()
            except Exception:
                pass


# ═══════════════════════════════════════════════════════════════════════
# 数据源: Bing — v10: 5 页深搜 + 合理 sleep
# ═══════════════════════════════════════════════════════════════════════

BING_QUERIES = [
    # US Navy 驱逐舰
    "USS Arleigh Burke DDG-51 destroyer photo",
    "USS Halsey DDG-97 destroyer photo",
    "USS Gridley DDG-101 destroyer photo",
    "USS Bainbridge DDG-96 destroyer photo",
    "USS Sampson DDG-102 destroyer photo",
    "USS Truxtun DDG-103 destroyer photo",
    "USS Sterett DDG-104 destroyer photo",
    "USS Dewey DDG-105 destroyer photo",
    "USS Stockdale DDG-106 destroyer photo",
    "USS Gravely DDG-107 destroyer photo",
    "USS Wayne E. Meyer DDG-108 destroyer photo",
    "USS Jason Dunham DDG-109 destroyer photo",
    "USS William P. Lawrence DDG-110 destroyer photo",
    "USS Spruance DDG-111 destroyer photo",
    "USS Michael Murphy DDG-112 destroyer photo",
    "USS John Finn DDG-113 destroyer photo",
    "USS Ralph Johnson DDG-114 destroyer photo",
    "USS Rafael Peralta DDG-115 destroyer photo",
    "USS Thomas Hudner DDG-116 destroyer photo",
    "USS Paul Ignatius DDG-117 destroyer photo",
    # 巡洋舰
    "USS Bunker Hill CG-52 cruiser photo",
    "USS Mobile Bay CG-53 cruiser photo",
    "USS Antietam CG-54 cruiser photo",
    "USS Leyte Gulf CG-55 cruiser photo",
    "USS San Jacinto CG-56 cruiser photo",
    "USS Lake Champlain CG-57 cruiser photo",
    "USS Philippine Sea CG-58 cruiser photo",
    "USS Princeton CG-59 cruiser photo",
    "USS Normandy CG-60 cruiser photo",
    "USS Chancellorsville CG-62 cruiser photo",
    "USS Cowpens CG-63 cruiser photo",
    "USS Gettysburg CG-64 cruiser photo",
    "USS Chosin CG-65 cruiser photo",
    "USS Hue City CG-66 cruiser photo",
    "USS Shiloh CG-67 cruiser photo",
    "USS Anzio CG-68 cruiser photo",
    "USS Vicksburg CG-69 cruiser photo",
    "USS Lake Erie CG-70 cruiser photo",
    "USS Cape St. George CG-71 cruiser photo",
    "USS Vella Gulf CG-72 cruiser photo",
    "USS Port Royal CG-73 cruiser photo",
    # 航母
    "USS Nimitz CVN-68 aircraft carrier photo",
    "USS Dwight D. Eisenhower CVN-69 photo",
    "USS Carl Vinson CVN-70 photo",
    "USS Theodore Roosevelt CVN-71 photo",
    "USS Abraham Lincoln CVN-72 photo",
    "USS John C. Stennis CVN-74 photo",
    "USS Harry S. Truman CVN-75 photo",
    "USS Ronald Reagan CVN-76 photo",
    "USS George H.W. Bush CVN-77 photo",
    "USS Gerald R. Ford CVN-78 photo",
    # 两栖
    "USS America LHA-6 photo",
    "USS Wasp LHD-1 photo",
    "USS Essex LHD-2 photo",
    "USS Kearsarge LHD-3 photo",
    "USS Boxer LHD-4 photo",
    "USS Bataan LHD-5 photo",
    "USS Iwo Jima LHD-7 photo",
    "USS Makin Island LHD-8 photo",
    "USS San Antonio LPD-17 photo",
    # 护卫舰
    "USS Oliver Hazard Perry FFG-7 photo",
    # 潜艇
    "USS Virginia SSN-774 submarine photo",
    "USS Seawolf SSN-21 submarine photo",
    "USS Los Angeles SSN-688 submarine photo",
    "USS Ohio SSBN-726 submarine photo",
    # LCS
    "USS Freedom LCS-1 photo",
    "USS Independence LCS-2 photo",
    # 国际海军
    "HMS Defender D36 destroyer photo",
    "HMS Duncan D37 destroyer photo",
    "HMS Dragon D35 destroyer photo",
    "HMS Diamond D34 destroyer photo",
    "Royal Navy Type 45 destroyer D36 photo",
    "HMS Queen Elizabeth R08 carrier photo",
    "HMS Prince of Wales R09 carrier photo",
    "French Navy FREMM frigate photo",
    "Charles de Gaulle R91 carrier photo",
    "FS Forbin D620 destroyer photo",
    "FS Chevalier Paul D621 photo",
    "German Navy F125 frigate photo",
    "FGS Sachsen F219 frigate photo",
    "FGS Hamburg F220 photo",
    "Japanese JS Izumo DDH-183 photo",
    "Japanese JS Kaga DDH-184 photo",
    "JS Atago DDG-177 photo",
    "JS Ashigara DDG-178 photo",
    "JS Myoko DDG-175 photo",
    "JS Chokai DDG-176 photo",
    "Korean Navy destroyer KDX-III photo",
    "ROK Sejong the Great DDG-991 photo",
    "ROK Yulgok Yi DDG-992 photo",
    "Indian Navy destroyer Kolkata class photo",
    "INS Kolkata D63 destroyer photo",
    "INS Kochi D64 destroyer photo",
    "Indian Navy Vikramaditya carrier photo",
    "Italian Navy aircraft carrier Cavour photo",
    "ITS Andrea Doria D553 photo",
    "ITS Caio Duilio D554 photo",
    "Spanish Navy Juan Carlos I LHD photo",
    "Australian Navy Hobart class destroyer photo",
    "HMAS Hobart DDG-39 photo",
    "HMAS Brisbane DDG-41 photo",
    "Canadian Navy Halifax class frigate photo",
    "HMCS Halifax FFH330 photo",
    "HMCS Vancouver FFH331 photo",
    "HNLMS De Zeven Provincien F802 photo",
    "HNLMS Tromp F803 photo",
    "HNoMS Fridtjof Nansen F310 photo",
    "HNoMS Roald Amundsen F311 photo",
    "HTMS Naresuan F421 frigate photo",
    "HTMS Taksin F422 photo",
    "KD Lekiu F30 frigate photo",
    "KD Jebat F29 frigate photo",
    "KRI Martadinata F331 frigate photo",
    "KRI I Gusti Ngurah F332 photo",
    # 中国
    "Type 052D destroyer DDG hull number photo",
    "Type 055 destroyer DDG hull number photo",
    "Type 054A frigate FFG hull number photo",
    "Chinese navy destroyer hull number photo",
    # 通用
    "navy destroyer pennant number side view",
    "warship hull number side view photo",
    "guided missile destroyer DDG underway photo",
    "frigate FFG underway photo",
    "navy ship number bow photo",
    "destroyer bow number close photo",
    "warship hull number starboard side photo",
    "navy pennant number close side view",
    "cruiser CG hull number side photo",
    "carrier island number close photo",
    "amphibious ship hull number side photo",
    "submarine sail number side photo",
    "guided missile destroyer hull number close view",
    "navy ship side number high resolution photo",
    "DDG hull number side view official navy photo",
    "FFG hull number side view official navy photo",
    "CG hull number side view official navy photo",
    "LHD hull number side view official navy photo",
    "LPD hull number side view official navy photo",
    "SSN hull number side view official navy photo",
    "Type 052D hull number side photo",
    "Type 055 hull number side photo",
    "Type 054A hull number side photo",
    "Royal Navy pennant number side photo",
    "French Navy pennant number side photo",
    "German Navy pennant number side photo",
    "Japanese Navy pennant number side photo",
    "Korean Navy pennant number side photo",
    "Indian Navy pennant number side photo",
    "Italian Navy pennant number side photo",
    "Spanish Navy pennant number side photo",
    "Australian Navy pennant number side photo",
    "Canadian Navy pennant number side photo",
    "Japanese navy destroyer hull number DDG photo",
    "Korean navy destroyer hull number KDX photo",
    "Indian navy destroyer hull number D63 D64 photo",
    "German navy frigate hull number F219 F220 photo",
    "French navy destroyer hull number D620 D621 photo",
    "Italian navy destroyer hull number D553 D554 photo",
    "Spanish navy frigate hull number F100 F101 photo",
    "Australian navy destroyer hull number DDG photo",
    "Canadian navy frigate hull number FFH photo",
    "Netherlands navy frigate hull number F802 F803 photo",
    "Norwegian navy frigate hull number F310 F311 photo",
    "Taiwan Navy destroyer hull number photo",
    "Singapore Navy frigate Formidable class photo",
    "Thai Navy frigate hull number photo",
    "Malaysian Navy frigate Lekiu class photo",
    "Indonesian Navy frigate Martadinata class photo",
    "Brazilian Navy frigate hull number photo",
    "Dutch Navy De Zeven Provincien frigate photo",
    "Norwegian Navy Fridtjof Nansen frigate photo",
    "Turkish Navy frigate hull number photo",
    "Greek Navy frigate hull number photo",
]


class BingSource:
    name = "bing"
    prefix = "bi"

    def __init__(self, session_pool: SessionPool, config: Config):
        self.pool = session_pool
        self.config = config

    def get_candidates(self, max_results: int = 10000, dedup: DedupManager | None = None) -> list[CandidateImage]:
        candidates: list[CandidateImage] = []
        seen_images: set[str] = set()
        # v10: Bing 翻 5 页 (原 3 页)
        bing_pages = (1, 35, 71, 105, 141)

        for query in BING_QUERIES:
            if len(candidates) >= max_results:
                break
            try:
                results = self._search(query, dedup, seen_images, bing_pages, max_per_query=80)
                candidates.extend(results)
                if len(results) > 0:
                    print(f"  [Bing] '{query[:50]}': +{len(results)} (total: {len(candidates)})")
            except Exception:
                pass
            time.sleep(0.3)  # v10: 0.05→0.3, 尊重 Bing rate limit

        return candidates[:max_results]

    def _search(self, query: str, dedup: DedupManager | None,
                seen_images: set[str], pages: tuple, max_per_query: int = 80) -> list[CandidateImage]:
        results: list[CandidateImage] = []
        for first in pages:
            if len(results) >= max_per_query:
                break
            url = f"https://www.bing.com/images/search?q={quote(query)}&form=HDRSC2&first={first}"
            try:
                s = self.pool.get()
                resp = s.get(url, timeout=self.config.request_timeout)
                resp.raise_for_status()
                for meta in self._extract_bing_meta(resp.text):
                    image_url = meta.get("murl", "")
                    page_url = meta.get("purl", "")
                    if not image_url or image_url in seen_images:
                        continue
                    if is_watermark_domain(image_url) or is_watermark_domain(page_url):
                        continue
                    if is_watermark_image_url(image_url):
                        continue
                    if dedup and dedup.is_duplicate(image_url, page_url):
                        continue
                    seen_images.add(image_url)
                    source_id = hashlib.md5(image_url.encode()).hexdigest()[:16]
                    results.append(CandidateImage(
                        source=self.name,
                        page_url=page_url,
                        image_url=image_url,
                        source_id=source_id,
                        title=meta.get("t", ""),
                        has_hull_in_title=title_has_hull_number(meta.get("t", "")),
                    ))
                    if len(results) >= max_per_query:
                        break
            except Exception:
                continue
        return results

    @staticmethod
    def _extract_bing_meta(html: str) -> list[dict]:
        results = []
        for raw in re.findall(r'\bm="(\{&quot;.*?\})"', html):
            try:
                from html import unescape
                payload = json.loads(unescape(raw))
            except json.JSONDecodeError:
                continue
            if payload.get("murl"):
                results.append(payload)
        return results


# ═══════════════════════════════════════════════════════════════════════
# 数据源: USNI via Bing — v10: 减少页面数降低代理压力
# ═══════════════════════════════════════════════════════════════════════

USNI_BING_QUERIES = [
    "site:photos.usni.org destroyer DDG",
    "site:photos.usni.org cruiser CG",
    "site:photos.usni.org aircraft carrier CVN",
    "site:photos.usni.org frigate FFG",
    "site:photos.usni.org amphibious LHD",
    "site:photos.usni.org submarine SSN",
    "site:photos.usni.org littoral combat ship",
    "site:photos.usni.org coast guard cutter",
    "site:photos.usni.org USS destroyer",
    "site:photos.usni.org guided missile",
    "site:photos.usni.org navy ship hull",
    "site:photos.usni.org warship",
]


class USNISource:
    name = "usni"
    prefix = "un"

    def __init__(self, session_pool: SessionPool, config: Config):
        self.pool = session_pool
        self.config = config

    def get_candidates(self, max_results: int = 10000, dedup: DedupManager | None = None) -> list[CandidateImage]:
        candidates: list[CandidateImage] = []
        seen_images: set[str] = set()
        usni_pages = (1, 35, 71, 105)  # 4 页

        for query in USNI_BING_QUERIES:
            if len(candidates) >= max_results:
                break
            try:
                results = self._search(query, dedup, seen_images, usni_pages)
                candidates.extend(results)
                print(f"  [USNI] '{query[:40]}': +{len(results)} (total: {len(candidates)})")
            except Exception:
                pass
            time.sleep(0.2)

        return candidates[:max_results]

    def _search(self, query: str, dedup: DedupManager | None,
                seen_images: set[str], pages: tuple) -> list[CandidateImage]:
        results: list[CandidateImage] = []
        for first in pages:
            url = f"https://www.bing.com/images/search?q={quote(query)}&form=HDRSC2&first={first}"
            try:
                s = self.pool.get()
                resp = s.get(url, timeout=self.config.request_timeout)
                resp.raise_for_status()
                for meta in BingSource._extract_bing_meta(resp.text):
                    image_url = meta.get("murl", "")
                    page_url = meta.get("purl", "")
                    if not image_url or image_url in seen_images:
                        continue
                    if "usni.org" not in (page_url + image_url).lower():
                        continue
                    if dedup and dedup.is_duplicate(image_url, page_url):
                        continue
                    seen_images.add(image_url)
                    source_id = hashlib.md5(image_url.encode()).hexdigest()[:16]
                    results.append(CandidateImage(
                        source=self.name,
                        page_url=page_url,
                        image_url=image_url,
                        source_id=source_id,
                        title=meta.get("t", ""),
                        has_hull_in_title=title_has_hull_number(meta.get("t", "")),
                    ))
            except Exception:
                continue
        return results


# ═══════════════════════════════════════════════════════════════════════
# 数据源: NavSource (直连)
# ═══════════════════════════════════════════════════════════════════════

NAVSOURCE_INDEX_URLS = [
    "http://www.navsource.org/archives/05/0502.htm",
    "http://www.navsource.org/archives/05/0504.htm",
    "http://www.navsource.org/archives/04/0402.htm",
    "http://www.navsource.org/archives/02/0202.htm",
    "http://www.navsource.org/archives/08/0802.htm",
    "http://www.navsource.org/archives/10/1002.htm",
    "http://www.navsource.org/archives/09/0902.htm",
    "http://www.navsource.org/archives/12/1202.htm",
]

NAVSOURCE_BLOCKED_PATHS = (
    "/contact", "/links", "/faq", "/about", "/updates",
    "/whatsnew", "/search", "/help", "/privacy",
)


class NavSourceSource:
    name = "navsource"
    prefix = "ns"

    def __init__(self, session_pool: SessionPool, config: Config):
        self.pool = session_pool
        self.config = config

    def get_candidates(self, max_results: int = 10000, dedup: DedupManager | None = None) -> list[CandidateImage]:
        all_ship_urls: list[str] = []
        seen_urls: set[str] = set()

        print("  [NavSource] Phase 1: Discovering ship pages...")
        for idx, index_url in enumerate(NAVSOURCE_INDEX_URLS):
            try:
                ship_urls = self._discover_ship_pages(index_url, seen_urls)
                all_ship_urls.extend(ship_urls)
                short = index_url.split("/")[-1][:50]
                print(f"  [NavSource] [{idx+1}/{len(NAVSOURCE_INDEX_URLS)}] {short}: +{len(ship_urls)} (total: {len(all_ship_urls)})")
            except Exception as e:
                print(f"  [NavSource] Error on {index_url[:60]}: {e}")
            time.sleep(0.1)

        print(f"  [NavSource] Total ship pages: {len(all_ship_urls)}")

        print("  [NavSource] Phase 2: Extracting images from ship pages...")
        candidates: list[CandidateImage] = []
        seen_images: set[str] = set()

        executor = ThreadPoolExecutor(max_workers=self.config.mq_fetch_workers)
        try:
            futures = {}
            for ship_url in all_ship_urls[:max_results * 2]:
                s = self.pool.get()
                futures[executor.submit(self._extract_images, s, ship_url, dedup, seen_images)] = ship_url

            pending = set(futures.keys())
            while pending:
                done, pending = wait(pending, timeout=60)
                for future in done:
                    if len(candidates) >= max_results:
                        break
                    try:
                        page_cands = future.result()
                        if page_cands:
                            candidates.extend(page_cands)
                    except Exception:
                        pass
                if not done:
                    for f in pending:
                        f.cancel()
                    pending.clear()
                    break
        finally:
            executor.shutdown(wait=False)

        print(f"  [NavSource] Total candidates: {len(candidates)}")
        return candidates[:max_results]

    def _discover_ship_pages(self, index_url: str, seen: set[str]) -> list[str]:
        ship_urls: list[str] = []
        try:
            s = self.pool.get()
            resp = s.get(index_url, timeout=self.config.request_timeout)
            resp.raise_for_status()
        except Exception:
            return ship_urls

        soup = BeautifulSoup(resp.text, _BS4_PARSER)
        for link in soup.select("a[href]"):
            href = link.get("href", "").strip()
            if not href:
                continue
            absolute = urljoin(index_url, href)
            if absolute in seen:
                continue
            lower = absolute.lower()
            if any(t in lower for t in NAVSOURCE_BLOCKED_PATHS):
                continue
            if not lower.endswith((".htm", ".html")):
                continue
            if "archives" in lower and any(t in lower for t in ("/05/", "/04/", "/02/", "/08/", "/10/", "/09/", "/12/")):
                seen.add(absolute)
                ship_urls.append(absolute)
        return ship_urls

    def _extract_images(self, session: requests.Session, ship_url: str,
                        dedup: DedupManager | None, seen_images: set[str]) -> list[CandidateImage]:
        try:
            resp = session.get(ship_url, timeout=self.config.request_timeout)
            resp.raise_for_status()
        except Exception:
            return []

        soup = BeautifulSoup(resp.text, _BS4_PARSER)
        title = soup.title.get_text(" ", strip=True) if soup.title else ""
        has_hull = title_has_hull_number(title) or url_has_hull_number(ship_url)
        results: list[CandidateImage] = []

        for img in soup.select("img[src]"):
            src = img.get("src", "").strip()
            if not src:
                continue
            image_url = urljoin(ship_url, src)
            if image_url in seen_images:
                continue
            lower = image_url.lower()
            if not lower.endswith((".jpg", ".jpeg", ".png", ".webp", ".gif")):
                continue
            if any(t in lower for t in ("banner", "navigate", "flag", "logo", "button", "icon", "line", "divider")):
                continue
            if is_watermark_image_url(image_url):
                continue
            if dedup and dedup.is_duplicate(image_url, ship_url):
                continue

            w_attr = img.get("width")
            h_attr = img.get("height")
            if w_attr and h_attr:
                try:
                    if int(str(w_attr).replace("px", "")) < 250 or int(str(h_attr).replace("px", "")) < 150:
                        continue
                except (ValueError, TypeError):
                    pass

            seen_images.add(image_url)
            source_id = hashlib.md5(image_url.encode()).hexdigest()[:16]
            results.append(CandidateImage(
                source=self.name,
                page_url=ship_url,
                image_url=image_url,
                source_id=source_id,
                title=title,
                has_hull_in_title=has_hull,
            ))
        return results


# ═══════════════════════════════════════════════════════════════════════
# 数据源: MaritimeQuest (直连) — v10: 每 worker 独立 session
# ═══════════════════════════════════════════════════════════════════════

MQ_INDEX_URLS = [
    "https://www.maritimequest.com/warship_directory/us_navy_pages/destroyers/us_navy_destroyer_hull_number_index.htm",
    "https://www.maritimequest.com/warship_directory/us_navy_pages/destroyers/us_navy_dd_hull_index.htm",
    "https://www.maritimequest.com/warship_directory/us_navy_pages/destroyers/us_navy_ddg_hull_number_index.htm",
    "https://www.maritimequest.com/warship_directory/us_navy_pages/destroyers/us_navy_destroyer_leader_hull_number_index.htm",
    "http://mail.maritimequest.com/warship_directory/us_navy_pages/aircraft_carriers/us_aircraft_carrier_index.htm",
    "http://mail.maritimequest.com/warship_directory/us_navy_pages/amphibious/amphibious_ship_index.htm",
    "https://www.maritimequest.com/warship_directory/us_navy_pages/cruisers/us_navy_cruiser_hull_number_index.htm",
    "https://www.maritimequest.com/warship_directory/us_navy_pages/frigates/us_navy_frigate_hull_number_index.htm",
    "https://www.maritimequest.com/warship_directory/us_navy_pages/submarines/us_navy_submarine_hull_number_index.htm",
    "https://www.maritimequest.com/warship_directory/china/pages/chinese_navy_main_page.htm",
    "https://www.maritimequest.com/warship_directory/russia/russian_navy_index.htm",
    "https://www.maritimequest.com/warship_directory/japan/japanese_navy_main.htm",
    "https://www.maritimequest.com/warship_directory/great_britain/pages/royal_navy_main_page.htm",
    "https://www.maritimequest.com/warship_directory/germany/german_navy_main.htm",
    "https://www.maritimequest.com/warship_directory/france/french_navy_main.htm",
    "https://www.maritimequest.com/warship_directory/australia/royal_australian_navy_main_page.htm",
    "https://www.maritimequest.com/warship_directory/canada/royal_canadian_navy_main.htm",
    "https://www.maritimequest.com/warship_directory/republic_of_korea/rok_index.htm",
    "https://www.maritimequest.com/warship_directory/spain/pages/spanish_navy_index.htm",
    "https://www.maritimequest.com/misc_ships/misc_ship_index.htm",
    "https://www.maritimequest.com/warship_directory/us_navy_pages/coast_guard/us_coast_guard_index.htm",
    "https://www.maritimequest.com/warship_directory/us_navy_pages/littorial_combat_ships/lcs_index.htm",
]

MQ_BLOCKED_PATHS = (
    "/misc_pages/", "/monuments", "/memorial", "/site_info_pages/",
    "/alphabetical_listing/", "/daily_event", "/library", "/in_the_news",
    "/links", "/photo_gallery", "/miscellaneous",
)


class MaritimeQuestSource:
    name = "maritimequest"
    prefix = "mq"

    def __init__(self, session_pool: SessionPool, config: Config):
        self.pool = session_pool
        self.config = config

    def get_candidates(self, max_results: int = 10000, dedup: DedupManager | None = None) -> list[CandidateImage]:
        all_ship_urls: list[str] = []
        seen_urls: set[str] = set()

        print("  [MQ] Phase 1: Discovering ship pages...")
        for idx, index_url in enumerate(MQ_INDEX_URLS):
            try:
                ship_urls = self._discover_ship_pages(index_url, seen_urls)
                all_ship_urls.extend(ship_urls)
                short = index_url.split("/")[-1][:50]
                print(f"  [MQ] [{idx+1}/{len(MQ_INDEX_URLS)}] {short}: +{len(ship_urls)} (total: {len(all_ship_urls)})")
            except Exception as e:
                print(f"  [MQ] Error on {index_url[:60]}: {e}")
            time.sleep(0.1)

        print(f"  [MQ] Total ship pages: {len(all_ship_urls)}")

        print("  [MQ] Phase 2: Extracting images from ship pages (parallel)...")
        candidates: list[CandidateImage] = []
        seen_images: set[str] = set()

        fetch_urls = all_ship_urls[:max_results * 3]
        executor = ThreadPoolExecutor(max_workers=self.config.mq_fetch_workers)
        try:
            futures = {}
            for ship_url in fetch_urls:
                s = self.pool.get()
                futures[executor.submit(self._extract_images, s, ship_url, dedup, seen_images)] = ship_url

            pending = set(futures.keys())
            while pending:
                done, pending = wait(pending, timeout=60)
                for future in done:
                    if len(candidates) >= max_results:
                        break
                    try:
                        page_cands = future.result()
                        if page_cands:
                            candidates.extend(page_cands)
                    except Exception:
                        pass
                if not done:
                    for f in pending:
                        f.cancel()
                    pending.clear()
                    break
        finally:
            executor.shutdown(wait=False)

        print(f"  [MQ] Total candidates: {len(candidates)}")
        return candidates[:max_results]

    def _discover_ship_pages(self, index_url: str, seen: set[str]) -> list[str]:
        ship_urls: list[str] = []
        queue = [index_url]
        visited: set[str] = {index_url}

        s = self.pool.get()
        while queue and len(ship_urls) < 500:
            url = queue.pop(0)
            try:
                resp = s.get(url, timeout=self.config.request_timeout)
                resp.raise_for_status()
            except Exception:
                continue

            soup = BeautifulSoup(resp.text, _BS4_PARSER)
            for link in soup.select("a[href]"):
                href = link.get("href", "").strip()
                if not href:
                    continue
                absolute = urljoin(url, href)
                if absolute in seen or absolute in visited:
                    continue
                if not absolute.lower().endswith((".htm", ".html")):
                    continue
                if any(t in absolute.lower() for t in MQ_BLOCKED_PATHS):
                    continue

                if self._is_ship_page(absolute):
                    seen.add(absolute)
                    ship_urls.append(absolute)
                elif self._is_index_page(absolute) and absolute not in visited:
                    visited.add(absolute)
                    queue.append(absolute)

        return ship_urls

    def _extract_images(self, session: requests.Session, ship_url: str,
                        dedup: DedupManager | None, seen_images: set[str]) -> list[CandidateImage]:
        try:
            resp = session.get(ship_url, timeout=self.config.request_timeout)
            resp.raise_for_status()
        except Exception:
            return []

        soup = BeautifulSoup(resp.text, _BS4_PARSER)
        title = soup.title.get_text(" ", strip=True) if soup.title else ""
        has_hull = title_has_hull_number(title) or url_has_hull_number(ship_url)
        results: list[CandidateImage] = []

        for img in soup.select("img[src]"):
            src = img.get("src", "").strip()
            if not src:
                continue
            image_url = urljoin(ship_url, src)
            if image_url in seen_images:
                continue
            lower = image_url.lower()
            if not lower.endswith((".jpg", ".jpeg", ".png", ".webp")):
                continue
            if any(t in lower for t in ("banner", "navigate", "flag", "logo", "button", "icon")):
                continue
            if is_watermark_image_url(image_url):
                continue
            if dedup and dedup.is_duplicate(image_url, ship_url):
                continue

            w_attr = img.get("width")
            h_attr = img.get("height")
            if w_attr and h_attr:
                try:
                    if int(str(w_attr).replace("px", "")) < 250 or int(str(h_attr).replace("px", "")) < 150:
                        continue
                except (ValueError, TypeError):
                    pass

            seen_images.add(image_url)
            source_id = hashlib.md5(image_url.encode()).hexdigest()[:16]
            results.append(CandidateImage(
                source=self.name,
                page_url=ship_url,
                image_url=image_url,
                source_id=source_id,
                title=title,
                has_hull_in_title=has_hull,
            ))
        return results

    @staticmethod
    def _is_ship_page(url: str) -> bool:
        lower = url.lower()
        if not lower.endswith((".htm", ".html")):
            return False
        if any(t in lower for t in ("overview", "index", "main_page", "contact", "links", "recent_updates")):
            if "/misc_ships/" not in lower:
                return False
        return any(t in lower for t in ("/pages/", "uss_", "_page_", "/misc_ships/"))

    @staticmethod
    def _is_index_page(url: str) -> bool:
        lower = url.lower()
        return any(t in lower for t in (
            "index", "main_page", "_main.htm", "hull_index",
            "hull_number_index", "alpha_index", "ship_index",
            "dd_index_alpha_pages", "class_overview",
        ))


# ═══════════════════════════════════════════════════════════════════════
# 数据源: Wikimedia Commons
# ═══════════════════════════════════════════════════════════════════════

WIKIMEDIA_QUERIES = [
    "USS destroyer DDG hull number",
    "guided missile destroyer hull number",
    "frigate FFG navy hull",
    "aircraft carrier CVN navy",
    "submarine SSN navy hull",
    "cruiser CG navy hull",
    "amphibious LHD navy",
    "coast guard cutter WHEC WMEC",
    "Type 052 destroyer Chinese navy",
    "Type 055 destroyer Chinese navy",
    "Type 054A frigate Chinese navy",
    "Royal Navy destroyer Type 45",
    "HMS destroyer D36 D37 D35",
    "French navy frigate FREMM",
    "German navy frigate F125 F124",
    "Japanese destroyer JMSDF DDH",
    "Korean navy destroyer KDX",
    "Indian navy destroyer Kolkata",
    "Italian navy frigate FREMM",
    "Spanish navy frigate",
    "Australian navy destroyer Hobart",
    "Canadian navy frigate Halifax",
    "Dutch navy frigate De Zeven",
    "Norwegian navy frigate Nansen",
    "Turkish navy frigate",
    "warship pennant number side",
    "navy ship hull number bow",
    "destroyer underway bow number",
    "frigate underway side number",
    "guided missile destroyer DDG-51",
    "Littoral Combat Ship LCS navy",
    "LPD amphibious transport navy",
    "LHA amphibious assault navy",
    "SSBN ballistic missile submarine",
    "patrol boat hull number navy",
]


class WikimediaSource:
    name = "wikimedia"
    prefix = "wk"

    def __init__(self, session_pool: SessionPool, config: Config):
        self.pool = session_pool
        self.config = config

    def get_candidates(self, max_results: int = 10000, dedup: DedupManager | None = None) -> list[CandidateImage]:
        candidates: list[CandidateImage] = []
        seen_images: set[str] = set()

        for query in WIKIMEDIA_QUERIES:
            if len(candidates) >= max_results:
                break
            try:
                results = self._search(query, dedup, seen_images)
                candidates.extend(results)
                if results:
                    print(f"  [Wiki] '{query[:40]}': +{len(results)} (total: {len(candidates)})")
            except Exception:
                pass
            time.sleep(0.3)

        return candidates[:max_results]

    def _search(self, query: str, dedup: DedupManager | None,
                seen_images: set[str]) -> list[CandidateImage]:
        results: list[CandidateImage] = []
        sroffset = 0
        while len(results) < 100 and sroffset < 200:
            api = (
                "https://commons.wikimedia.org/w/api.php"
                f"?action=query&list=search&srsearch={quote(query)}"
                "&srnamespace=6&srlimit=50&format=json"
                + (f"&sroffset={sroffset}" if sroffset else "")
            )
            try:
                s = self.pool.get()
                resp = s.get(api, timeout=self.config.request_timeout)
                if resp.status_code == 429:
                    time.sleep(3)
                    continue
                resp.raise_for_status()
                data = resp.json()
            except Exception:
                break

            search_results = data.get("query", {}).get("search", [])
            if not search_results:
                break

            titles = [r["title"] for r in search_results if r["title"].startswith("File:")]
            if titles:
                img_urls = self._get_image_urls(titles)
                for title, img_url in img_urls.items():
                    if img_url in seen_images:
                        continue
                    if is_watermark_image_url(img_url):
                        continue
                    if dedup and dedup.is_duplicate(img_url):
                        continue
                    seen_images.add(img_url)
                    source_id = hashlib.md5(img_url.encode()).hexdigest()[:16]
                    results.append(CandidateImage(
                        source=self.name,
                        page_url=f"https://commons.wikimedia.org/wiki/{quote(title)}",
                        image_url=img_url,
                        source_id=source_id,
                        title=title.replace("File:", "").replace(".jpg", "").replace(".png", ""),
                        has_hull_in_title=title_has_hull_number(title),
                    ))

            cont = data.get("continue", {})
            if "sroffset" not in cont:
                break
            sroffset = cont["sroffset"]
            time.sleep(0.2)

        return results

    def _get_image_urls(self, titles: list[str]) -> dict[str, str]:
        result = {}
        for i in range(0, len(titles), 50):
            batch = titles[i:i+50]
            titles_param = "|".join(batch)
            api = (
                "https://commons.wikimedia.org/w/api.php"
                f"?action=query&titles={quote(titles_param)}"
                "&prop=imageinfo&iiprop=url|size&iiurlwidth=1024&format=json"
            )
            try:
                s = self.pool.get()
                resp = s.get(api, timeout=self.config.request_timeout)
                if resp.status_code == 429:
                    time.sleep(3)
                    continue
                resp.raise_for_status()
                data = resp.json()
                pages = data.get("query", {}).get("pages", {})
                for page_id, page_data in pages.items():
                    title = page_data.get("title", "")
                    imageinfo = page_data.get("imageinfo", [{}])
                    if imageinfo:
                        url = imageinfo[0].get("thumburl") or imageinfo[0].get("url", "")
                        width = imageinfo[0].get("width", 0)
                        height = imageinfo[0].get("height", 0)
                        if url and width >= 500 and height >= 350:
                            result[title] = url
            except Exception:
                continue
            time.sleep(0.1)
        return result


# ═══════════════════════════════════════════════════════════════════════
# 主爬虫 v10
# ═══════════════════════════════════════════════════════════════════════

class ShipScraperV10:
    def __init__(self, config: Config):
        self.config = config
        self.output_dir = config.output_dir
        self.workspace_root = Path(r"E:\guangdianbishe")
        self._setup_dirs()

        self.dedup = DedupManager(self.workspace_root, self.output_dir)

        # Session 池: 直连池和代理池分开
        self._resolved_proxy = ""
        if config.proxy == "auto":
            self._resolved_proxy = auto_detect_proxy()
        elif config.proxy:
            self._resolved_proxy = config.proxy

        # 直连池: MQ, NavSource (无代理, 较高并发)
        self.direct_pool = SessionPool(proxy_url="", pool_size=config.direct_workers + 2)
        # 代理池: Bing, USNI, Wiki (经代理, 低并发)
        self.proxy_pool = SessionPool(proxy_url=self._resolved_proxy, pool_size=config.proxy_workers + 2)

        self.accepted_sha1: set[str] = set()
        self.accepted_dhash: set[str] = set()
        self.accepted_rows: list[dict] = []
        self.accepted_image_urls: set[str] = set()
        self.accepted_page_counts: dict[str, int] = defaultdict(int)  # v10: 同页计数
        self._load_state()

        self.rejection_counts: dict[str, int] = defaultdict(int)
        self.ocr_stats = {"ocr_passed": 0, "ocr_failed": 0, "metadata_fast_path": 0}
        self.batch_stats_history: list[dict] = []
        self.lock = Lock()
        self.start_time: float = 0.0
        self.last_review_accepted: int = 0
        self.failed_urls: set[str] = set()
        self.stall_retry_counts: dict[str, int] = defaultdict(int)
        self.candidate_status: dict[str, str] = {}

    def _setup_dirs(self):
        for sub in ("raw", "processed", "reports", "state", "review", "config"):
            (self.output_dir / sub).mkdir(parents=True, exist_ok=True)

    def _load_state(self):
        csv_path = self.output_dir / "reports" / "accepted.csv"
        if csv_path.exists():
            try:
                with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
                    self.accepted_rows = list(csv.DictReader(f))
                for row in self.accepted_rows:
                    sha1 = (row.get("sha1") or "").strip()
                    dhash = (row.get("dhash") or "").strip()
                    img_url = (row.get("image_url") or "").strip()
                    pg_url = (row.get("page_url") or "").strip()
                    if sha1:
                        self.accepted_sha1.add(sha1)
                    if dhash:
                        self.accepted_dhash.add(dhash)
                    if img_url:
                        self.accepted_image_urls.add(img_url)
                    if pg_url:
                        self.accepted_page_counts[pg_url] += 1
                print(f"[State] Loaded {len(self.accepted_rows)} accepted from {csv_path}")
            except Exception as e:
                print(f"[State] Error loading accepted.csv: {e}")

        state_path = self.output_dir / "state" / "scraper_state.json"
        if state_path.exists():
            try:
                state = json.loads(state_path.read_text(encoding="utf-8"))
                self.rejection_counts = defaultdict(int, state.get("rejection_counts", {}))
                self.ocr_stats = defaultdict(int, state.get("ocr_stats", {}))
                pg = state.get("accepted_page_counts", {})
                self.accepted_page_counts = defaultdict(int, pg)
                print(f"[State] Loaded state from {state_path}")
            except Exception as e:
                print(f"[State] Error loading state: {e}")

        self._reconcile_orphans()

    def _reconcile_orphans(self):
        processed_dir = self.output_dir / "processed"
        raw_dir = self.output_dir / "raw"
        if not processed_dir.is_dir():
            return
        accepted_names = {r["file_name"] for r in self.accepted_rows}
        orphans = []
        for f in processed_dir.iterdir():
            if f.is_file() and f.name not in accepted_names:
                orphans.append(f)
        if orphans:
            for f in orphans:
                safe_unlink(f)
                stem = f.stem
                for ext in (".jpg", ".jpeg", ".png", ".webp", ".bmp"):
                    raw_file = raw_dir / f"{stem}{ext}"
                    if raw_file.exists():
                        safe_unlink(raw_file)
                        break
            print(f"[Reconcile] Removed {len(orphans)} orphan files")

    def _save_state(self):
        csv_path = self.output_dir / "reports" / "accepted.csv"
        fieldnames = [
            "file_name", "source", "page_url", "image_url", "title",
            "orig_width", "orig_height", "sharpness", "ocr_text",
            "ocr_conf", "sha1", "dhash", "hull_in_title",
        ]
        with csv_path.open("w", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(self.accepted_rows)

        state_path = self.output_dir / "state" / "scraper_state.json"
        state = {
            "accepted_count": len(self.accepted_rows),
            "accepted_sha1": sorted(self.accepted_sha1),
            "accepted_dhash": sorted(self.accepted_dhash),
            "rejection_counts": dict(self.rejection_counts),
            "ocr_stats": dict(self.ocr_stats),
            "accepted_page_counts": dict(self.accepted_page_counts),
            "last_save": datetime.now().isoformat(),
        }
        state_path.write_text(json.dumps(state, indent=2, ensure_ascii=False), encoding="utf-8")

    def _reject(self, reason: str):
        with self.lock:
            self.rejection_counts[reason] = self.rejection_counts.get(reason, 0) + 1

    # ── 并行候选发现 ─────────────────────────────────────────────────

    def _get_pool_for_source(self, name: str) -> SessionPool:
        """直连源用 direct_pool, 代理源用 proxy_pool"""
        if name in ("maritimequest", "navsource"):
            return self.direct_pool
        return self.proxy_pool

    def _discover_all(self) -> list[CandidateImage]:
        all_cands: list[CandidateImage] = []
        source_order = [
            ("bing", BingSource),
            ("usni", USNISource),
            ("navsource", NavSourceSource),
            ("maritimequest", MaritimeQuestSource),
            ("wikimedia", WikimediaSource),
        ]

        active_sources = [(name, cls) for name, cls in source_order if name in self.config.sources]

        executor = ThreadPoolExecutor(max_workers=min(self.config.discovery_workers, len(active_sources)))
        try:
            futures = {}
            for name, cls in active_sources:
                def _run_source(src_name=name, src_cls=cls):
                    if self.config.time_limit > 0 and (time.time() - self.start_time) > self.config.time_limit:
                        return src_name, []
                    print(f"\n--- {src_name} ---")
                    try:
                        pool = self._get_pool_for_source(src_name)
                        src = src_cls(pool, self.config)
                        cands = src.get_candidates(
                            max_results=self.config.max_per_source, dedup=self.dedup
                        )
                        print(f"  {src_name}: {len(cands)} candidates")
                        return src_name, cands
                    except Exception as e:
                        print(f"  {src_name} error: {e}")
                        return src_name, []

                futures[executor.submit(_run_source)] = name

            pending_sources = set(futures.keys())
            while pending_sources:
                done, pending_sources = wait(pending_sources, timeout=60)
                for future in done:
                    try:
                        src_name, cands = future.result()
                        all_cands.extend(cands)
                        if src_name in ("bing", "usni") and len(all_cands) >= self.config.batch_size * 5:
                            print(f"\n[Discovery] {len(all_cands)} fast candidates, short-circuiting...")
                            for f in pending_sources:
                                f.cancel()
                            pending_sources.clear()
                            break
                    except Exception as e:
                        print(f"  Discovery worker error: {e}")
                if not done:
                    for f in pending_sources:
                        f.cancel()
                    pending_sources.clear()
                    break
        finally:
            executor.shutdown(wait=False)

        random.Random(self.config.seed).shuffle(all_cands)
        return all_cands

    def _save_candidates(self, candidates: list[CandidateImage]):
        path = self.output_dir / "reports" / "source_candidates.csv"
        with path.open("w", encoding="utf-8-sig", newline="") as f:
            w = csv.DictWriter(f, fieldnames=["source", "image_url", "page_url", "title", "has_hull_in_title"])
            w.writeheader()
            for c in candidates:
                w.writerow({
                    "source": c.source, "image_url": c.image_url,
                    "page_url": c.page_url, "title": c.title,
                    "has_hull_in_title": c.has_hull_in_title,
                })

    def _load_cached_candidates(self) -> Optional[list[CandidateImage]]:
        path = self.output_dir / "reports" / "source_candidates.csv"
        if not path.exists():
            return None
        try:
            candidates = []
            with path.open("r", encoding="utf-8-sig", newline="") as f:
                for row in csv.DictReader(f):
                    candidates.append(CandidateImage(
                        source=row.get("source", ""),
                        image_url=row.get("image_url", ""),
                        page_url=row.get("page_url", ""),
                        title=row.get("title", ""),
                        source_id=hashlib.md5(row.get("image_url", "").encode()).hexdigest()[:16],
                        has_hull_in_title=row.get("has_hull_in_title", "") == "True",
                    ))
            print(f"[Cache] Loaded {len(candidates)} cached candidates from {path}")
            return candidates
        except Exception as e:
            print(f"[Cache] Failed to load cached candidates: {e}")
            return None

    def _prioritize_candidates(self, candidates: list[CandidateImage]) -> list[CandidateImage]:
        source_rank = {"maritimequest": 0, "bing": 1, "usni": 2, "navsource": 3, "wikimedia": 4}

        def priority(c: CandidateImage):
            metadata_has_hull = (
                c.has_hull_in_title
                or url_has_hull_number(c.image_url)
                or url_has_hull_number(c.page_url)
            )
            return (0 if metadata_has_hull else 1, source_rank.get(c.source, 9))

        return sorted(candidates, key=priority)

    # ── 单张图片处理 (下载+过滤, 不含OCR) ──────────────────────────

    def _download_and_prefilter(self, cand: CandidateImage) -> tuple[Path | None, dict | None]:
        """下载 + 非OCR过滤。返回 (临时路径, 元数据) 或 (None, None)."""
        config = self.config
        raw_dir = self.output_dir / "raw"
        prefix = {"bing": "bi", "usni": "un", "maritimequest": "mq",
                  "wikimedia": "wk", "navsource": "ns"}.get(cand.source, "xx")
        tmp_path = raw_dir / f"tmp_{prefix}_{cand.source_id}.bin"

        # v10: 同页限制检查
        page_url = cand.page_url
        with self.lock:
            if self.accepted_page_counts.get(page_url, 0) >= config.per_page_limit:
                self._reject("per_page_limit")
                return None, None

        # 下载 (指数退避重试)
        for attempt in range(3):
            try:
                # 选择合适的 session 池
                pool = self._get_pool_for_source(cand.source)
                s = pool.get()
                resp = s.get(cand.image_url, timeout=config.request_timeout, stream=False)
                resp.raise_for_status()
                ct = resp.headers.get("Content-Type", "").lower()
                if ct and not ct.startswith("image/"):
                    self._reject("non_image_content_type")
                    return None, None
                tmp_path.write_bytes(resp.content)
                break
            except Exception:
                safe_unlink(tmp_path)
                if attempt == 2:
                    self._reject("download_failed")
                    return None, None
                time.sleep(1 * (attempt + 1))

        if not tmp_path.exists():
            return None, None

        # 文件大小
        if tmp_path.stat().st_size < config.min_file_size:
            safe_unlink(tmp_path)
            self._reject("too_small_file")
            return None, None

        # 图片格式
        img_type = imghdr.what(tmp_path)
        if img_type not in ("jpeg", "png", "webp", "bmp"):
            safe_unlink(tmp_path)
            self._reject("invalid_format")
            return None, None

        # SHA1 去重
        file_sha1 = image_sha1(tmp_path)
        with self.lock:
            if file_sha1 in self.accepted_sha1 or self.dedup.is_sha1_dup(file_sha1):
                safe_unlink(tmp_path)
                self._reject("duplicate_sha1")
                return None, None

        # 尺寸
        try:
            from PIL import Image
            with Image.open(tmp_path) as img:
                w, h = img.size
        except Exception:
            safe_unlink(tmp_path)
            self._reject("corrupt_image")
            return None, None
        if w < config.min_width or h < config.min_height:
            safe_unlink(tmp_path)
            self._reject("too_small_dims")
            return None, None

        # 清晰度
        sharpness = check_sharpness(tmp_path)
        if sharpness < config.sharpness_threshold:
            safe_unlink(tmp_path)
            self._reject("blurry")
            return None, None

        # dHash 近似去重
        dhash = image_dhash(tmp_path)
        with self.lock:
            if any(hamming_distance(dhash, old) <= 4 for old in self.accepted_dhash):
                safe_unlink(tmp_path)
                self._reject("near_duplicate")
                return None, None

        metadata = {
            "cand": cand, "file_sha1": file_sha1, "dhash": dhash,
            "w": w, "h": h, "sharpness": sharpness, "img_type": img_type,
        }
        return tmp_path, metadata

    # ── 批量处理 (下载 + batch OCR + 保存) ────────────────────────

    def _process_batches(self, candidates: list[CandidateImage], target: int) -> int:
        config = self.config
        current = len(self.accepted_rows)

        pending = [c for c in candidates
                   if c.image_url not in self.accepted_image_urls
                   and c.image_url not in self.failed_urls]
        print(f"Pending (after dedup, excluding {len(self.failed_urls)} failed): {len(pending)}")

        batch_idx = 0
        while current < target and pending:
            if config.time_limit > 0 and (time.time() - self.start_time) > config.time_limit:
                print(f"\n[Time limit: {config.time_limit}s] Stopping.")
                break

            batch = pending[:config.batch_size]
            pending = pending[config.batch_size:]
            batch_idx += 1
            batch_start_time = time.time()
            accepted_before = current
            rejection_before = dict(self.rejection_counts)

            print(f"\n{'─'*50}")
            print(f"Batch {batch_idx} | {len(batch)} candidates | accepted: {current}/{target}")
            print(f"{'─'*50}")

            # ── Phase A: 并行下载 + 非OCR过滤 ─────
            # v10: 根据源类型动态选择并发数
            # 把候选分为直连和代理两类, 分别控制并发
            direct_batch = [c for c in batch if c.source in ("maritimequest", "navsource")]
            proxy_batch = [c for c in batch if c.source not in ("maritimequest", "navsource")]

            prefiltered: list[tuple[Path, dict]] = []
            download_lock = Lock()

            def download_worker(cand: CandidateImage) -> tuple[CandidateImage, Path | None, dict | None]:
                tmp, meta = self._download_and_prefilter(cand)
                return cand, tmp, meta

            # 混合并发: 直连6 + 代理4 = 总共最多10
            # v10.1: 用 wait(timeout) 替代 as_completed, 防止 hung future 永久阻塞
            all_futures: dict[Future, CandidateImage] = {}
            with ThreadPoolExecutor(max_workers=config.direct_workers + config.proxy_workers) as dl_exec:
                for cand in batch:
                    future = dl_exec.submit(download_worker, cand)
                    all_futures[future] = cand

                pending_dl: set[Future] = set(all_futures.keys())
                stall_count = 0
                while pending_dl:
                    if current + len(prefiltered) + len(self.accepted_rows) - accepted_before >= target:
                        for f in pending_dl:
                            f.cancel()
                        break

                    done, still_pending = wait(pending_dl, timeout=60)

                    if not done and still_pending:
                        # Hung futures detected — resubmit, don't block forever
                        hung = len(still_pending)
                        requeued = 0
                        for f in list(still_pending):
                            cand = all_futures[f]
                            self.stall_retry_counts[cand.image_url] += 1
                            if self.stall_retry_counts[cand.image_url] <= config.max_stall_retries:
                                new_f = dl_exec.submit(download_worker, cand)
                                all_futures[new_f] = cand
                                requeued += 1
                            else:
                                self.failed_urls.add(cand.image_url)
                            f.cancel()
                            still_pending.discard(f)
                        stall_count += 1
                        print(f"  [Stall] {hung} inflight hung. Requeued {requeued}, "
                              f"exhausted: {hung - requeued}. Stall count: {stall_count}")
                        if stall_count >= 5:
                            print(f"  [Stall] Too many stalls ({stall_count}), breaking batch {batch_idx}")
                            for f in still_pending:
                                f.cancel()
                            break
                        pending_dl = still_pending  # will be empty since we discarded all
                        continue

                    for future in done:
                        try:
                            cand_result, tmp, meta = future.result(timeout=5)
                        except Exception:
                            cand_result = all_futures[future]
                            self.stall_retry_counts[cand_result.image_url] += 1
                            if self.stall_retry_counts[cand_result.image_url] <= config.max_stall_retries:
                                new_f = dl_exec.submit(download_worker, cand_result)
                                all_futures[new_f] = cand_result
                            else:
                                self.failed_urls.add(cand_result.image_url)
                            continue

                        if tmp is not None and meta is not None:
                            with download_lock:
                                prefiltered.append((tmp, meta))
                                if len(prefiltered) % 20 == 0:
                                    print(f"  ... prefiltered {len(prefiltered)} in batch {batch_idx}")

                    pending_dl = still_pending

            print(f"  Batch {batch_idx}: {len(prefiltered)} passed prefilter (from {len(batch)} candidates)")

            # ── Phase B: batch OCR + 最终保存 ─────
            if prefiltered:
                accepted_in_batch = self._batch_ocr_and_save(prefiltered, target - current)

            current = len(self.accepted_rows)
            self._save_state()

            elapsed = time.time() - batch_start_time
            new_accepted = current - accepted_before

            batch_rejections = {}
            for reason, count in self.rejection_counts.items():
                delta = count - rejection_before.get(reason, 0)
                if delta > 0:
                    batch_rejections[reason] = delta

            print(f"\nBatch {batch_idx} complete:")
            print(f"  New accepted: {new_accepted} | Total: {current}")
            print(f"  Elapsed: {elapsed:.0f}s | Per accepted: {elapsed/new_accepted:.1f}s" if new_accepted > 0 else f"  Elapsed: {elapsed:.0f}s")
            print(f"  Rejection Top 5: {sorted(batch_rejections.items(), key=lambda x: -x[1])[:5]}")

            total_elapsed = time.time() - self.start_time
            rate = current / (total_elapsed / 3600) if total_elapsed > 0 else 0
            print(f"  Overall rate: {rate:.1f} accepted/hour | Remaining: {target - current}")

            if new_accepted > 0 and (current - self.last_review_accepted) >= config.review_interval:
                self._do_review()

            if not pending and current < target:
                print(f"\n[Discovery] Queue empty. Re-discovering...")
                try:
                    new_cands = self._discover_all()
                    fresh = [c for c in new_cands
                             if c.image_url not in self.accepted_image_urls
                             and c.image_url not in self.failed_urls]
                    print(f"[Discovery] Found {len(new_cands)} new, {len(fresh)} after dedup.")
                    pending = fresh
                    self._save_candidates(self._load_or_create_candidates() + fresh)
                except Exception as e:
                    print(f"[Discovery] Error: {e}")

        return current

    def _batch_ocr_and_save(self, prefiltered: list[tuple[Path, dict]], max_accept: int) -> int:
        """v10: batch OCR inference + save. Returns number accepted."""
        config = self.config
        accepted = 0
        raw_dir = self.output_dir / "raw"

        # 先处理 metadata fast path (不需要OCR)
        need_ocr: list[tuple[Path, dict]] = []
        for tmp_path, meta in prefiltered:
            cand = meta["cand"]
            metadata_has_hull = (
                cand.has_hull_in_title
                or url_has_hull_number(cand.image_url)
                or url_has_hull_number(cand.page_url)
            )
            if metadata_has_hull:
                # Fast path: skip OCR
                meta["ocr_ok"] = True
                meta["ocr_text"] = "[metadata_hull_match]"
                meta["ocr_conf"] = 1.0
                with self.lock:
                    self.ocr_stats["ocr_passed"] += 1
                    self.ocr_stats["metadata_fast_path"] += 1
            else:
                need_ocr.append((tmp_path, meta))

        # batch OCR for remaining
        if need_ocr:
            for i in range(0, len(need_ocr), config.ocr_batch_size):
                batch = need_ocr[i:i + config.ocr_batch_size]
                batch_paths = [p for p, _ in batch]
                ocr_results = batch_ocr_check(
                    batch_paths, config.min_ocr_digits, config.max_ocr_chars,
                    gpu=config.gpu_ocr,
                )
                for j, (ocr_ok, ocr_text, ocr_conf) in enumerate(ocr_results):
                    _, meta = batch[j]
                    meta["ocr_ok"] = ocr_ok
                    meta["ocr_text"] = ocr_text
                    meta["ocr_conf"] = ocr_conf
                    if ocr_ok:
                        with self.lock:
                            self.ocr_stats["ocr_passed"] += 1
                    else:
                        with self.lock:
                            self.ocr_stats["ocr_failed"] += 1

        # 保存所有通过的图片
        for tmp_path, meta in prefiltered:
            if accepted >= max_accept:
                safe_unlink(tmp_path)
                continue

            if not meta.get("ocr_ok"):
                safe_unlink(tmp_path)
                self._reject("no_digits_or_too_long")
                continue

            cand = meta["cand"]
            file_sha1 = meta["file_sha1"]
            dhash = meta["dhash"]
            w, h = meta["w"], meta["h"]
            sharpness = meta["sharpness"]
            img_type = meta["img_type"]
            prefix = {"bing": "bi", "usni": "un", "maritimequest": "mq",
                      "wikimedia": "wk", "navsource": "ns"}.get(cand.source, "xx")

            safe_title = "".join(ch if ch.isalnum() else "_" for ch in (cand.title or "")[:25].lower()).strip("_")
            stem = f"{prefix}_{cand.source_id}_{safe_title or cand.source}"
            proc_path = self.output_dir / "processed" / f"{stem}.jpg"
            raw_final = raw_dir / f"{stem}.{img_type if img_type != 'jpeg' else 'jpg'}"

            try:
                normalize_to_jpeg(tmp_path, proc_path, config.target_width, config.target_height)
                tmp_path.rename(raw_final)
            except Exception:
                safe_unlink(tmp_path)
                safe_unlink(proc_path)
                self._reject("normalize_failed")
                continue

            # 更新索引
            with self.lock:
                self.accepted_sha1.add(file_sha1)
                if dhash:
                    self.accepted_dhash.add(dhash)
                self.accepted_image_urls.add(cand.image_url)
                self.accepted_page_counts[cand.page_url] += 1
                self.dedup.add(cand.image_url, cand.page_url, file_sha1)

            row = {
                "file_name": proc_path.name,
                "source": cand.source,
                "page_url": cand.page_url,
                "image_url": cand.image_url,
                "title": cand.title,
                "orig_width": w, "orig_height": h,
                "sharpness": round(sharpness, 2),
                "ocr_text": str(meta.get("ocr_text", ""))[:100],
                "ocr_conf": round(float(meta.get("ocr_conf", 0)), 4),
                "sha1": file_sha1, "dhash": dhash,
                "hull_in_title": str(cand.has_hull_in_title),
            }
            self.accepted_rows.append(row)
            accepted += 1

        return accepted

    # ── 候选管理 ─────────────────────────────────────────────────────

    def _load_or_create_candidates(self) -> list[CandidateImage]:
        cached = self._load_cached_candidates()
        if cached:
            return cached
        return []

    # ── 质量抽检 ─────────────────────────────────────────────────────

    def _do_review(self):
        if len(self.accepted_rows) <= self.last_review_accepted:
            return
        new_rows = self.accepted_rows[self.last_review_accepted:]
        sample_size = min(30, len(new_rows))
        sampled = random.Random(self.config.seed).sample(new_rows, sample_size)

        review_dir = self.output_dir / "review"
        processed_dir = self.output_dir / "processed"

        review_csv = review_dir / f"review_{len(self.accepted_rows)}.csv"
        with review_csv.open("w", encoding="utf-8-sig", newline="") as f:
            fields = ["file_name", "source", "page_url", "image_url", "title", "ocr_text", "hull_in_title"]
            w = csv.DictWriter(f, fieldnames=fields)
            w.writeheader()
            for row in sampled:
                w.writerow({k: row.get(k, "") for k in fields})
                src = processed_dir / row["file_name"]
                if src.exists():
                    shutil.copy2(src, review_dir / row["file_name"])

        self.last_review_accepted = len(self.accepted_rows)
        print(f"\n[Review] Sampled {sample_size} images to {review_csv}")

    # ── 数据集切分 ───────────────────────────────────────────────────

    def _split_dataset(self):
        proc = self.output_dir / "processed"
        train_dir = self.output_dir / "train" / "images"
        test_dir = self.output_dir / "test" / "images"
        for d in (train_dir, test_dir):
            d.mkdir(parents=True, exist_ok=True)
            for f in d.glob("*.jpg"):
                f.unlink()
        images = sorted(proc.glob("*.jpg"))
        random.Random(self.config.seed).shuffle(images)
        cut = max(1, int(len(images) * 0.85)) if len(images) > 1 else len(images)
        for img in images[:cut]:
            shutil.copy2(img, train_dir / img.name)
        for img in images[cut:]:
            shutil.copy2(img, test_dir / img.name)
        cfg_dir = self.output_dir / "config"
        cfg_dir.mkdir(exist_ok=True)
        (cfg_dir / "classes.txt").write_text("hull_number\n", encoding="utf-8")
        (cfg_dir / "ship_hull.yaml").write_text(
            f"path: {self.output_dir.resolve().as_posix()}\n"
            "train: train/images\nval: test/images\nnc: 1\nnames: ['hull_number']\n",
            encoding="utf-8")
        print(f"Split: {cut} train, {len(images) - cut} test")

    # ── 运行报告 ─────────────────────────────────────────────────────

    def _generate_report(self, total_elapsed: float) -> dict:
        current = len(self.accepted_rows)
        proc_count = len(list((self.output_dir / "processed").glob("*.jpg")))
        rejection_top5 = sorted(self.rejection_counts.items(), key=lambda x: -x[1])[:5]

        cand_csv = self.output_dir / "reports" / "source_candidates.csv"
        total_candidates = 0
        if cand_csv.exists():
            try:
                with cand_csv.open("r", encoding="utf-8-sig") as f:
                    total_candidates = sum(1 for _ in f) - 1
            except Exception:
                pass

        metadata_pct = 0
        if self.ocr_stats["ocr_passed"] > 0:
            metadata_pct = self.ocr_stats["metadata_fast_path"] / self.ocr_stats["ocr_passed"] * 100

        rate = current / (total_elapsed / 3600) if total_elapsed > 0 else 0

        return {
            "version": "v10",
            "timestamp": datetime.now().isoformat(),
            "output_dir": str(self.output_dir),
            "duration_sec": round(total_elapsed, 1),
            "target": self.config.target,
            "sources": list(self.config.sources),
            "proxy": self._resolved_proxy or "none",
            "gpu_ocr": self.config.gpu_ocr,
            "direct_workers": self.config.direct_workers,
            "proxy_workers": self.config.proxy_workers,
            "total_candidates": total_candidates,
            "accepted_csv_count": current,
            "processed_files": proc_count,
            "accepted_per_hour": round(rate, 1),
            "remaining_to_target": self.config.target - current,
            "ocr_passed": self.ocr_stats["ocr_passed"],
            "ocr_failed": self.ocr_stats["ocr_failed"],
            "metadata_fast_path": self.ocr_stats["metadata_fast_path"],
            "metadata_pct": round(metadata_pct, 1),
            "rejection_top5": rejection_top5,
            "per_page_limit": self.config.per_page_limit,
        }

    def _save_report(self, report: dict):
        json_path = self.output_dir / "reports" / "summary.json"
        json_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")

        ts = datetime.now().strftime("%Y%m%d-%H%M")
        md_path = self.output_dir / "reports" / f"run_report_{ts}.md"
        md = f"""# Ship Scraper v10 运行报告

## 基本信息

- 版本: v10 (独立Session池 + 分源并发 + batch OCR)
- 时间: {report['timestamp']}
- 输出目录: {report['output_dir']}
- 运行时长: {report['duration_sec']:.0f}s ({report['duration_sec']/3600:.1f}h)
- 数据源: {', '.join(report['sources'])}
- 代理: {report['proxy']}
- GPU OCR: {report['gpu_ocr']}
- 并发: 直连{report['direct_workers']} / 代理{report['proxy_workers']}

## 数量结果

- 候选总数: {report['total_candidates']}
- accepted 总数: {report['accepted_csv_count']}
- 每小时 accepted: {report['accepted_per_hour']}
- 距 {report['target']} 差距: {report['remaining_to_target']}

## 质量结果

- OCR 真通过: {report['ocr_passed']}
- metadata fast path: {report['metadata_fast_path']}
- metadata 占比: {report['metadata_pct']}%
- OCR 失败: {report['ocr_failed']}

## 拒绝原因 Top 5

"""
        for i, (reason, count) in enumerate(report['rejection_top5'], 1):
            md += f"{i}. {reason}: {count}\n"

        md += f"""
## 配置

- per_page_limit: {report['per_page_limit']}
"""
        md_path.write_text(md, encoding="utf-8")
        print(f"\n[Report] Saved to {md_path}")

    # ── 主入口 ───────────────────────────────────────────────────────

    def run(self):
        config = self.config
        current = len(self.accepted_rows)
        target = config.target
        self.start_time = time.time()

        print(f"\n{'='*60}")
        print(f"Ship Scraper v10")
        print(f"Target: {target} | Already accepted: {current} | Need: {target - current}")
        print(f"Sources: {', '.join(config.sources)}")
        print(f"Proxy: {self._resolved_proxy or 'none (direct)'}")
        print(f"GPU OCR: {config.gpu_ocr} (batch size: {config.ocr_batch_size})")
        print(f"Concurrency: direct={config.direct_workers}, proxy={config.proxy_workers}")
        print(f"Per-page limit: {config.per_page_limit}")
        print(f"Time limit: {config.time_limit}s ({config.time_limit/3600:.1f}h)")
        print(f"Output: {self.output_dir}")
        print(f"{'='*60}\n")

        if current >= target:
            print("Target already reached!")
            report = self._generate_report(0.0)
            self._save_report(report)
            return report

        # 候选发现 / 缓存加载
        cached = self._load_cached_candidates()
        if cached:
            all_candidates = cached
            print(f"[Cache] Using {len(all_candidates)} cached candidates.")
            try:
                new_cands = self._discover_all()
                existing_urls = {c.image_url for c in all_candidates}
                new_unique = [c for c in new_cands if c.image_url not in existing_urls]
                all_candidates.extend(new_unique)
                print(f"[Discovery] Added {len(new_unique)} new candidates to cache.")
                self._save_candidates(all_candidates)
            except Exception as e:
                print(f"[Discovery] Error during supplementary discovery: {e}")
        else:
            all_candidates = self._discover_all()
            if not all_candidates:
                print("ERROR: No candidates found!")
                print("  > Check proxy or try --no-proxy for direct-only sources.")
                return self._generate_report(time.time() - self.start_time)
            self._save_candidates(all_candidates)

        all_candidates = self._prioritize_candidates(all_candidates)
        print(f"\nTotal candidates: {len(all_candidates)}")

        final_count = self._process_batches(all_candidates, target)

        if final_count > 0:
            self._split_dataset()

        if len(self.accepted_rows) > self.last_review_accepted:
            self._do_review()

        # 清理
        self.direct_pool.close_all()
        self.proxy_pool.close_all()

        total_elapsed = time.time() - self.start_time
        report = self._generate_report(total_elapsed)
        self._save_report(report)

        return report


# ═══════════════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════════════

def main():
    import argparse
    parser = argparse.ArgumentParser(
        description="Ship Hull Number Image Scraper v10 — Session Pool + Split Concurrency + Batch OCR",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scraper_v10.py --target 5000 --time-limit 28800
  python scraper_v10.py --proxy http://127.0.0.1:7890
  python scraper_v10.py --no-proxy
  python scraper_v10.py --sources bing,maritimequest,navsource
  python scraper_v10.py --target 200 --time-limit 600  # 10min verification
        """
    )
    parser.add_argument("--output-dir", default=str(Path(r"E:\guangdianbishe\ship_scraper_v10\output")))
    parser.add_argument("--target", type=int, default=5000)
    parser.add_argument("--sources", default="bing,usni,navsource,maritimequest,wikimedia",
                        help="Comma-separated: bing,usni,navsource,maritimequest,wikimedia")
    parser.add_argument("--direct-workers", type=int, default=6,
                        help="Direct connection concurrency (MQ, NavSource)")
    parser.add_argument("--proxy-workers", type=int, default=4,
                        help="Proxy connection concurrency (Bing, USNI, Wiki)")
    parser.add_argument("--time-limit", type=int, default=0,
                        help="Seconds (0=unlimited)")
    parser.add_argument("--max-per-source", type=int, default=10000)
    parser.add_argument("--min-ocr-digits", type=int, default=1)
    parser.add_argument("--max-ocr-chars", type=int, default=15)
    parser.add_argument("--sharpness", type=float, default=60.0)
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--review-interval", type=int, default=100)
    parser.add_argument("--ocr-batch-size", type=int, default=6,
                        help="easyocr batch inference size (default: 6)")
    parser.add_argument("--per-page-limit", type=int, default=3,
                        help="Max accepted images per page_url (default: 3)")
    parser.add_argument("--proxy", default="auto",
                        help="Proxy URL. 'auto'=auto-detect (default), ''=direct")
    parser.add_argument("--no-proxy", action="store_true", help="Force direct connection")
    parser.add_argument("--no-gpu", action="store_true", help="Force CPU OCR")
    parser.add_argument("--discovery-workers", type=int, default=4)
    parser.add_argument("--mq-fetch-workers", type=int, default=6,
                        help="MQ Phase 2 ship page fetch workers")
    args = parser.parse_args()

    config = Config(
        output_dir=Path(args.output_dir),
        target=args.target,
        direct_workers=args.direct_workers,
        proxy_workers=args.proxy_workers,
        time_limit=args.time_limit,
        max_per_source=args.max_per_source,
        min_ocr_digits=args.min_ocr_digits,
        max_ocr_chars=args.max_ocr_chars,
        sharpness_threshold=args.sharpness,
        sources=tuple(s.strip() for s in args.sources.split(",")),
        batch_size=args.batch_size,
        review_interval=args.review_interval,
        ocr_batch_size=args.ocr_batch_size,
        per_page_limit=args.per_page_limit,
        proxy="" if args.no_proxy else args.proxy,
        gpu_ocr=not args.no_gpu,
        discovery_workers=args.discovery_workers,
        mq_fetch_workers=args.mq_fetch_workers,
    )

    scraper = ShipScraperV10(config)
    report = scraper.run()

    print(f"\n{'='*60}")
    print("FINAL SUMMARY")
    print(f"{'='*60}")
    for k, v in report.items():
        print(f"  {k}: {v}")


if __name__ == "__main__":
    main()
