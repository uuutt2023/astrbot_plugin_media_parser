"""B 站 buvid（设备指纹）获取与缓存。

## 为什么需要这个模块

B 站的 ``x/web-interface/view``、``x/web-interface/wbi/view`` 等接口在请求
**缺少 ``buvid3`` / ``buvid4`` Cookie** 时，返回的不是 JSON 错误码，而是
**412 Precondition Failed + 一坨风控 HTML 页面**（``<!DOCTYPE html>...``），
调用方只能报「API 返回非 JSON 响应」这种看不出所以然的错。

实测结论（2026-10）：

| 请求 | 缺 buvid | 带 buvid |
| --- | --- | --- |
| ``x/web-interface/view`` | 412 风控 HTML | 200 JSON |
| ``x/web-interface/wbi/view``（含 WBI 签名） | 412 风控 HTML | 200 JSON |
| ``x/player/pagelist`` | 200 JSON | 200 JSON |

**WBI 签名救不了这个校验** —— 带签名但无 buvid 依旧是 412。

``https://api.bilibili.com/x/frontend/finger/spi`` 可以**匿名**拿到一组
``buvid3`` / ``buvid4``，把它们当 Cookie 带上即可正常调用接口，**不需要登录**。

## 设计要点

- **惰性获取**：只有真正发请求需要时才去拉，不会拖慢启动
- **带 TTL 缓存**：buvid 是设备指纹，短时间复用即可，不必每次都打接口
- **失败不致命**：拉不到 buvid 就退回到「不带 buvid」，由调用方按原有逻辑处理，
  绝不让辅助能力反过来把主流程搞挂
- **不覆盖用户 Cookie**：用户自己配置的 Cookie 里若已有 buvid3/buvid4，以用户的为准

参考：

- https://github.com/SocialSisterYi/bilibili-API-collect/blob/master/docs/bilibili-api/buvid.md
- https://github.com/xiaoxi2760/astrbot_plugin_denia_share
- https://github.com/iris1598/astrbot_plugin_rika_share
"""
import asyncio
import time
from typing import Dict, Optional, Tuple

import aiohttp

from ...logger import logger

#: 匿名获取设备指纹的接口
SPI_API = "https://api.bilibili.com/x/frontend/finger/spi"

#: buvid 缓存有效期（秒）。buvid 是设备指纹而非会话凭证，短时复用即可；
#: 过长则可能拿到已被 B 站风控标记的指纹。
BUVID_TTL_SECONDS = 3600.0

#: 拉取 buvid 的超时（秒）
BUVID_TIMEOUT_SECONDS = 10.0

#: 参与风控的 Cookie 键名
BUVID_COOKIE_KEYS = ("buvid3", "buvid4")


class BuvidProvider:
    """匿名获取并缓存 B 站 buvid，提供 Cookie 合并能力。"""

    def __init__(self, ttl_seconds: float = BUVID_TTL_SECONDS):
        self._ttl_seconds = float(ttl_seconds)
        self._buvid3: str = ""
        self._buvid4: str = ""
        self._fetched_at: float = 0.0
        self._lock = asyncio.Lock()

    # ── 内部状态 ────────────────────────────────────────

    def _is_fresh(self) -> bool:
        """当前缓存是否仍在有效期内。"""
        if not self._buvid3:
            return False
        return (time.monotonic() - self._fetched_at) < self._ttl_seconds

    def _parse_spi(self, payload: Dict) -> Tuple[str, str]:
        """从 spi 接口响应里解析 buvid3 / buvid4。"""
        data = payload.get("data") or {}
        buvid3 = str(data.get("b_3") or "").strip()
        buvid4 = str(data.get("b_4") or "").strip()
        return buvid3, buvid4

    async def _fetch(self, session: aiohttp.ClientSession) -> Tuple[str, str]:
        """向 spi 接口请求一组 buvid。"""
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            ),
            "Referer": "https://www.bilibili.com/",
            "Accept": "application/json, text/plain, */*",
        }
        async with session.get(
            SPI_API,
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=BUVID_TIMEOUT_SECONDS),
        ) as resp:
            if resp.status != 200:
                raise RuntimeError(
                    f"获取B站buvid失败: HTTP {resp.status}"
                )
            payload = await resp.json(content_type=None)
        if not isinstance(payload, dict):
            raise RuntimeError("获取B站buvid失败: 响应不是JSON对象")
        return self._parse_spi(payload)

    # ── 对外接口 ────────────────────────────────────────

    async def get_buvid(self, session: aiohttp.ClientSession) -> Tuple[str, str]:
        """获取 (buvid3, buvid4)，失败时返回空串元组。"""
        if self._is_fresh():
            return self._buvid3, self._buvid4

        async with self._lock:
            # 双检：等锁期间可能已被别的协程填好了
            if self._is_fresh():
                return self._buvid3, self._buvid4
            try:
                buvid3, buvid4 = await self._fetch(session)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning(f"[bilibili] 获取buvid失败: {e}")
                return self._buvid3, self._buvid4

            if not buvid3:
                logger.warning("[bilibili] 获取buvid失败: 响应缺少 b_3")
                return self._buvid3, self._buvid4

            self._buvid3 = buvid3
            self._buvid4 = buvid4
            self._fetched_at = time.monotonic()
            logger.debug(
                f"[bilibili] 获取buvid成功: buvid3={buvid3[:12]}..., "
                f"buvid4={'有' if buvid4 else '无'}"
            )
            return self._buvid3, self._buvid4

    @staticmethod
    def parse_cookie(cookie_header: str) -> Dict[str, str]:
        """把 Cookie 请求头解析成字典（保留原始大小写键）。"""
        result: Dict[str, str] = {}
        for part in (cookie_header or "").split(";"):
            part = part.strip()
            if not part or "=" not in part:
                continue
            key, _, value = part.partition("=")
            key = key.strip()
            if key:
                result[key] = value.strip()
        return result

    @staticmethod
    def build_cookie(cookies: Dict[str, str]) -> str:
        """把 Cookie 字典拼回请求头。"""
        return "; ".join(f"{k}={v}" for k, v in cookies.items() if v)

    def has_buvid(self, cookie_header: str) -> bool:
        """判断现有 Cookie 是否已带 buvid3。"""
        cookies = self.parse_cookie(cookie_header)
        return any(cookies.get(key) for key in BUVID_COOKIE_KEYS)

    async def ensure_buvid(
        self,
        session: aiohttp.ClientSession,
        cookie_header: str = "",
    ) -> str:
        """确保 Cookie 里带得上 buvid，返回合并后的 Cookie 字符串。

        - 用户 Cookie 里已有 buvid3/buvid4 → 原样返回，不打扰用户凭证
        - 否则补进自动获取的 buvid（拿不到就保持原样）
        """
        cookies = self.parse_cookie(cookie_header)
        if any(cookies.get(key) for key in BUVID_COOKIE_KEYS):
            return cookie_header

        buvid3, buvid4 = await self.get_buvid(session)
        if not buvid3:
            return cookie_header

        cookies["buvid3"] = buvid3
        if buvid4:
            cookies["buvid4"] = buvid4
        return self.build_cookie(cookies)

    def invalidate(self) -> None:
        """清空缓存，强制下次重新获取。"""
        self._buvid3 = ""
        self._buvid4 = ""
        self._fetched_at = 0.0

    @property
    def cached(self) -> Optional[str]:
        """当前已缓存的 buvid3（未获取过则为 None）。"""
        return self._buvid3 or None
