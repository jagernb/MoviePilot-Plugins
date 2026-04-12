import re
import threading
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pytz
from apscheduler.schedulers.background import BackgroundScheduler

from app.core.config import settings
from app.core.event import eventmanager, Event
from app.log import logger
from app.plugins import _PluginBase
from app.schemas.types import EventType

try:
    from app.helper.mediaserver import MediaServerHelper
except Exception:  # pragma: no cover - runtime compatibility fallback
    MediaServerHelper = None

try:
    from app.schemas import RefreshMediaItem
except Exception:  # pragma: no cover - runtime compatibility fallback
    RefreshMediaItem = None


lock = threading.Lock()


class MediaServerRefresh(_PluginBase):
    plugin_name = "媒体库服务器刷新"
    plugin_desc = "整理完成后先等待 rclone 日志确认上传成功，再刷新 Emby/Jellyfin/Plex 媒体库。"
    plugin_icon = "refresh.png"
    plugin_version = "1.4.0"
    plugin_author = "jxxghp, Claude"
    author_url = "https://github.com/anthropics/claude-code"
    plugin_config_prefix = "mediaserverrefresh_"
    plugin_order = 66
    auth_level = 2

    _SUCCESS_MESSAGE = "vfs cache: upload succeeded"
    _MATCH_MODE_PATH_THEN_NAME = "path_then_name"
    _MATCH_MODE_PATH_ONLY = "path_only"
    _MATCH_MODE_NAME_ONLY = "name_only"

    _enabled: bool = False
    _onlyonce: bool = False
    _delay: int = 0
    _mediaservers: List[str] = []
    _rclone_log_path: str = ""
    _library_path_prefix: str = ""
    _log_poll_seconds: int = 15
    _success_pattern: str = r"vfs cache: upload succeeded"
    _match_mode: str = "path_then_name"
    _grace_delay: int = 5
    _pending_expire_minutes: int = 180
    _debug: bool = False
    _scheduler: Optional[BackgroundScheduler] = None

    def init_plugin(self, config: dict = None):
        self.stop_service()

        if config:
            self._enabled = bool(config.get("enabled", False))
            self._onlyonce = bool(config.get("onlyonce", False))
            self._delay = self.__safe_int(config.get("delay"), 0)
            self._mediaservers = config.get("mediaservers") or []
            self._rclone_log_path = (config.get("rclone_log_path") or "").strip()
            self._library_path_prefix = self.__normalize_prefix(config.get("library_path_prefix") or "")
            self._log_poll_seconds = max(5, self.__safe_int(config.get("log_poll_seconds"), 15))
            self._success_pattern = (config.get("success_pattern") or self._SUCCESS_MESSAGE).strip()
            self._match_mode = config.get("match_mode") or self._MATCH_MODE_PATH_THEN_NAME
            self._grace_delay = max(0, self.__safe_int(config.get("grace_delay"), 5))
            self._pending_expire_minutes = max(1, self.__safe_int(config.get("pending_expire_minutes"), 180))
            self._debug = bool(config.get("debug", False))

        if self._onlyonce:
            self._scheduler = BackgroundScheduler(timezone=settings.TZ)
            logger.info("媒体库服务器刷新服务启动，立即扫描一次 rclone 日志")
            self._scheduler.add_job(
                func=self.__poll_rclone_log,
                trigger="date",
                run_date=datetime.now(tz=pytz.timezone(settings.TZ)) + timedelta(seconds=3),
            )
            self._onlyonce = False
            self.__update_config()
            if self._scheduler.get_jobs():
                self._scheduler.print_jobs()
                self._scheduler.start()

    def __get_mediaserver_helper(self):
        if not MediaServerHelper:
            logger.error("[MediaServerRefresh] 无法导入 MediaServerHelper，当前 MoviePilot 版本可能不兼容")
            return None
        try:
            return MediaServerHelper()
        except Exception as err:
            logger.error("[MediaServerRefresh] 初始化 MediaServerHelper 失败：%s", err)
            return None

    @property
    def service_infos(self):
        helper = self.__get_mediaserver_helper()
        if not helper:
            return {}
        try:
            return helper.get_services(name_filters=self._mediaservers) or {}
        except Exception as err:
            logger.error("[MediaServerRefresh] 获取媒体服务器服务失败：%s", err)
            return {}

    def get_state(self) -> bool:
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        return []

    def get_api(self) -> List[Dict[str, Any]]:
        return []

    def get_service(self) -> List[Dict[str, Any]]:
        if not self._enabled:
            return []
        return [{
            "id": "MediaServerRefreshLogPoller",
            "name": "媒体库服务器刷新日志轮询服务",
            "trigger": "interval",
            "func": self.__poll_rclone_log,
            "kwargs": {"seconds": self._log_poll_seconds},
        }]

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        mediaserver_items = []
        helper = self.__get_mediaserver_helper()
        if helper:
            try:
                configs = helper.get_configs() or {}
                mediaserver_items = [
                    {"title": config.name, "value": name}
                    for name, config in configs.items()
                ]
            except Exception as err:
                logger.warning("[MediaServerRefresh] 获取媒体服务器配置失败：%s", err)

        return [
            {
                "component": "VForm",
                "content": [
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [{
                                    "component": "VSwitch",
                                    "props": {"model": "enabled", "label": "启用插件"}
                                }]
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [{
                                    "component": "VSwitch",
                                    "props": {"model": "debug", "label": "调试日志"}
                                }]
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [{
                                    "component": "VSwitch",
                                    "props": {"model": "onlyonce", "label": "立即扫描一次"}
                                }]
                            },
                        ]
                    },
                    {
                        "component": "VRow",
                        "content": [{
                            "component": "VCol",
                            "props": {"cols": 12},
                            "content": [{
                                "component": "VSelect",
                                "props": {
                                    "model": "mediaservers",
                                    "label": "媒体服务器",
                                    "items": mediaserver_items,
                                    "multiple": True,
                                    "chips": True,
                                    "clearable": True,
                                }
                            }]
                        }]
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [{
                                    "component": "VTextField",
                                    "props": {
                                        "model": "rclone_log_path",
                                        "label": "rclone 日志路径",
                                        "placeholder": "/path/to/rclone.log",
                                    }
                                }]
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [{
                                    "component": "VTextField",
                                    "props": {
                                        "model": "library_path_prefix",
                                        "label": "媒体库挂载前缀",
                                        "placeholder": "/CloudNAS/CloudDrive/rclone",
                                    }
                                }]
                            },
                        ]
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [{
                                    "component": "VTextField",
                                    "props": {
                                        "model": "log_poll_seconds",
                                        "label": "日志轮询秒数",
                                        "placeholder": "15",
                                    }
                                }]
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [{
                                    "component": "VTextField",
                                    "props": {
                                        "model": "grace_delay",
                                        "label": "上传成功后缓冲秒数",
                                        "placeholder": "5",
                                    }
                                }]
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [{
                                    "component": "VTextField",
                                    "props": {
                                        "model": "delay",
                                        "label": "批量去抖秒数",
                                        "placeholder": "0",
                                    }
                                }]
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [{
                                    "component": "VTextField",
                                    "props": {
                                        "model": "pending_expire_minutes",
                                        "label": "待刷新过期分钟",
                                        "placeholder": "180",
                                    }
                                }]
                            },
                        ]
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [{
                                    "component": "VTextField",
                                    "props": {
                                        "model": "success_pattern",
                                        "label": "成功日志匹配",
                                        "placeholder": "vfs cache: upload succeeded",
                                    }
                                }]
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [{
                                    "component": "VSelect",
                                    "props": {
                                        "model": "match_mode",
                                        "label": "匹配方式",
                                        "items": [
                                            {"title": "路径优先，文件名回退", "value": self._MATCH_MODE_PATH_THEN_NAME},
                                            {"title": "仅路径匹配", "value": self._MATCH_MODE_PATH_ONLY},
                                            {"title": "仅文件名匹配", "value": self._MATCH_MODE_NAME_ONLY},
                                        ],
                                    }
                                }]
                            },
                        ]
                    },
                    {
                        "component": "VRow",
                        "content": [{
                            "component": "VCol",
                            "props": {"cols": 12},
                            "content": [{
                                "component": "VAlert",
                                "props": {
                                    "type": "info",
                                    "variant": "tonal",
                                    "text": "插件只在 rclone 日志中检测到 upload succeeded 后才刷新媒体库，Copied (new) 不会触发刷新。"
                                }
                            }]
                        }]
                    }
                ]
            }
        ], {
            "enabled": False,
            "debug": False,
            "onlyonce": False,
            "mediaservers": [],
            "rclone_log_path": "",
            "library_path_prefix": "",
            "log_poll_seconds": 15,
            "success_pattern": self._SUCCESS_MESSAGE,
            "match_mode": self._MATCH_MODE_PATH_THEN_NAME,
            "grace_delay": 5,
            "delay": 0,
            "pending_expire_minutes": 180,
        }

    def get_page(self) -> List[dict]:
        return []

    def __load_pending_items(self) -> List[Dict[str, Any]]:
        return self.get_data("pending_items") or []

    def __save_pending_items(self, pending_items: List[Dict[str, Any]]):
        self.save_data("pending_items", pending_items)

    @eventmanager.register(EventType.TransferComplete)
    def refresh(self, event: Event = None):
        if not self._enabled or not event or not event.event_data:
            return

        event_info = event.event_data or {}
        transferinfo = event_info.get("transferinfo")
        mediainfo = event_info.get("mediainfo")
        if not transferinfo or not mediainfo:
            return

        target_path = self.__extract_target_path(transferinfo)
        if not target_path:
            if self._debug:
                logger.info("[MediaServerRefresh] 未从 transferinfo 提取到目标路径，跳过入队")
            return

        relative_path = self.__build_relative_media_path(target_path)
        filename = Path(target_path).name if target_path else ""
        now_ts = datetime.now(tz=pytz.timezone(settings.TZ)).isoformat()

        pending_item = {
            "title": getattr(mediainfo, "title", "") or getattr(mediainfo, "name", ""),
            "year": getattr(mediainfo, "year", None),
            "type": self.__serialize_value(getattr(mediainfo, "type", None)),
            "category": self.__serialize_value(getattr(mediainfo, "category", None)),
            "target_path": target_path,
            "relative_path": relative_path,
            "filename": filename,
            "added_at": now_ts,
            "uploaded_at": None,
            "refresh_after": None,
            "refreshed": False,
        }

        with lock:
            pending_items = self.__load_pending_items()
            existing_index = self.__find_pending_index(pending_items, target_path=target_path, relative_path=relative_path)
            if existing_index is not None:
                pending_items[existing_index].update(pending_item)
                action = "更新"
            else:
                pending_items.append(pending_item)
                action = "新增"
            self.__save_pending_items(pending_items)

        if self._debug:
            logger.info(
                "[MediaServerRefresh] %s pending item: target=%s relative=%s filename=%s",
                action,
                target_path,
                relative_path,
                filename,
            )

    def stop_service(self):
        try:
            if self._scheduler:
                self._scheduler.remove_all_jobs()
                if self._scheduler.running:
                    self._scheduler.shutdown()
                self._scheduler = None
        except Exception as err:
            logger.error("[MediaServerRefresh] 退出插件失败：%s", err)

    def __poll_rclone_log(self):
        if not self._enabled:
            return
        if not self._rclone_log_path:
            if self._debug:
                logger.info("[MediaServerRefresh] 未配置 rclone 日志路径，跳过轮询")
            return

        log_path = Path(self._rclone_log_path)
        try:
            stat = log_path.stat()
            if not log_path.is_file():
                logger.warning("[MediaServerRefresh] rclone 日志路径不是文件：%s", self._rclone_log_path)
                return
        except FileNotFoundError:
            logger.warning("[MediaServerRefresh] rclone 日志文件不存在：%s", self._rclone_log_path)
            return
        except Exception as err:
            logger.warning("[MediaServerRefresh] 读取日志文件状态失败：%s", err)
            return

        log_state = self.get_data("log_state") or {}
        offset = int(log_state.get("offset") or 0)
        last_size = int(log_state.get("size") or 0)
        current_size = int(stat.st_size)

        if offset > current_size or current_size < last_size:
            if self._debug:
                logger.info("[MediaServerRefresh] 检测到日志截断或轮转，重置 offset")
            offset = 0

        try:
            with log_path.open("r", encoding="utf-8", errors="ignore") as fh:
                fh.seek(offset)
                lines = []
                for line in fh:
                    lines.append(line)
                new_offset = fh.tell()
        except Exception as err:
            logger.warning("[MediaServerRefresh] 读取日志内容失败：%s", err)
            return

        state_changed = False
        success_paths = self.__extract_success_paths(lines)
        if success_paths:
            state_changed = self.__mark_uploaded_items(success_paths) or state_changed
        state_changed = self.__refresh_ready_items() or state_changed
        state_changed = self.__expire_pending_items() or state_changed

        new_log_state = {
            "offset": new_offset,
            "size": current_size,
            "mtime": int(stat.st_mtime),
        }
        if new_log_state != log_state:
            self.save_data("log_state", new_log_state)

    def __extract_success_paths(self, lines: List[str]) -> List[str]:
        matched: List[str] = []
        success_marker = (self._success_pattern or self._SUCCESS_MESSAGE).strip()
        for raw_line in lines:
            line = raw_line.strip()
            if not line or success_marker not in line:
                continue
            info_index = line.find("INFO  : ")
            if info_index < 0:
                continue
            payload = line[info_index + len("INFO  : "):]
            split_token = f": {success_marker}"
            split_index = payload.rfind(split_token)
            if split_index < 0:
                continue
            relative_path = self.__normalize_relative_path(payload[:split_index])
            if not relative_path:
                continue
            matched.append(relative_path)
            if self._debug:
                logger.info("[MediaServerRefresh] 命中上传成功日志：%s", relative_path)
        return matched

    def __mark_uploaded_items(self, success_paths: List[str]) -> bool:
        if not success_paths:
            return False

        now = datetime.now(tz=pytz.timezone(settings.TZ))
        consumed_keys = set(self.get_data("consumed_success_keys") or [])
        consumed_before = set(consumed_keys)

        with lock:
            pending_items = self.__load_pending_items()
            updated = False
            for success_path in success_paths:
                key = f"{success_path}|{now.strftime('%Y-%m-%d-%H-%M')}"
                matched_indexes = self.__find_matching_pending_indexes(pending_items, success_path)
                if not matched_indexes:
                    if self._debug:
                        logger.info("[MediaServerRefresh] 上传成功日志未匹配到 pending item：%s", success_path)
                    continue
                for index in matched_indexes:
                    item = pending_items[index]
                    if item.get("refreshed"):
                        continue
                    if item.get("uploaded_at") and key in consumed_keys:
                        continue
                    item["uploaded_at"] = now.isoformat()
                    item["refresh_after"] = (now + timedelta(seconds=self._grace_delay)).isoformat()
                    updated = True
                    if self._debug:
                        logger.info(
                            "[MediaServerRefresh] pending item 已匹配上传成功：target=%s relative=%s",
                            item.get("target_path"),
                            item.get("relative_path"),
                        )
                consumed_keys.add(key)
            if updated:
                self.__save_pending_items(pending_items)
            if consumed_keys != consumed_before:
                self.save_data("consumed_success_keys", self.__trim_consumed_keys(list(consumed_keys)))
        return updated or consumed_keys != consumed_before

    def __refresh_ready_items(self) -> bool:
        now = datetime.now(tz=pytz.timezone(settings.TZ))
        ready_items = []

        with lock:
            pending_items = self.__load_pending_items()
            for item in pending_items:
                if item.get("refreshed"):
                    continue
                if not item.get("uploaded_at"):
                    continue
                refresh_after = self.__parse_datetime(item.get("refresh_after"))
                if refresh_after and refresh_after > now:
                    continue
                ready_items.append(item)

        if not ready_items:
            return False

        if self._delay > 0:
            min_uploaded = min(filter(None, [self.__parse_datetime(item.get("uploaded_at")) for item in ready_items]), default=None)
            if min_uploaded and now < min_uploaded + timedelta(seconds=self._delay):
                return False

        services = self.service_infos
        if not services:
            return False

        refresh_items = [self.__build_refresh_media_item(item) for item in ready_items]
        refresh_items = [item for item in refresh_items if item is not None]
        if not refresh_items:
            return False

        refreshed = False
        for name, service in services.items():
            try:
                instance = getattr(service, "instance", service)
                if hasattr(instance, "refresh_library_by_items"):
                    instance.refresh_library_by_items(refresh_items)
                    refreshed = True
                    logger.info("[MediaServerRefresh] 已按条目刷新媒体服务器：%s，条目数=%s", name, len(refresh_items))
                elif hasattr(instance, "refresh_root_library"):
                    instance.refresh_root_library()
                    refreshed = True
                    logger.info("[MediaServerRefresh] 已刷新媒体服务器根库：%s", name)
                else:
                    logger.warning("[MediaServerRefresh] 媒体服务器 %s 不支持刷新接口", name)
            except Exception as err:
                logger.error("[MediaServerRefresh] 刷新媒体服务器 %s 失败：%s", name, err)

        if not refreshed:
            return False

        refreshed_paths = {item.get("target_path") for item in ready_items}
        with lock:
            pending_items = self.__load_pending_items()
            kept_items = []
            for item in pending_items:
                if item.get("target_path") in refreshed_paths:
                    continue
                kept_items.append(item)
            self.__save_pending_items(kept_items)
        return True

    def __expire_pending_items(self) -> bool:
        now = datetime.now(tz=pytz.timezone(settings.TZ))
        expiry = timedelta(minutes=self._pending_expire_minutes)
        expired_paths = []

        with lock:
            pending_items = self.__load_pending_items()
            kept_items = []
            for item in pending_items:
                if item.get("refreshed"):
                    continue
                added_at = self.__parse_datetime(item.get("added_at"))
                if not added_at or now - added_at <= expiry:
                    kept_items.append(item)
                    continue
                expired_paths.append(item.get("target_path") or item.get("relative_path") or item.get("filename"))
            changed = len(kept_items) != len(pending_items)
            if changed:
                self.__save_pending_items(kept_items)

        for path in expired_paths:
            logger.warning("[MediaServerRefresh] 待刷新项超时未等到 upload succeeded，已丢弃：%s", path)
        return changed

    def __build_refresh_media_item(self, item: Dict[str, Any]):
        if RefreshMediaItem:
            try:
                return RefreshMediaItem(
                    title=item.get("title"),
                    year=item.get("year"),
                    type=item.get("type"),
                    category=item.get("category"),
                    target_path=Path(item.get("target_path")),
                )
            except Exception as err:
                logger.warning("[MediaServerRefresh] 构造 RefreshMediaItem 失败，降级为字典：%s", err)
        return {
            "title": item.get("title"),
            "year": item.get("year"),
            "type": item.get("type"),
            "category": item.get("category"),
            "target_path": Path(item.get("target_path")) if item.get("target_path") else None,
        }

    def __extract_target_path(self, transferinfo: Any) -> str:
        candidate_attrs = [
            "target_path",
            "target_file",
            "target_filename",
            "dest",
            "dest_path",
            "destination",
            "path",
            "file_path",
        ]
        for field in candidate_attrs:
            value = getattr(transferinfo, field, None)
            value = self.__path_to_string(value)
            if value:
                return value

        target_diritem = getattr(transferinfo, "target_diritem", None)
        if target_diritem:
            value = self.__path_to_string(getattr(target_diritem, "path", None))
            if value:
                return value

        for field in ["target_dir", "dest_dir", "directory"]:
            base = self.__path_to_string(getattr(transferinfo, field, None))
            name = self.__path_to_string(getattr(transferinfo, "target_name", None) or getattr(transferinfo, "filename", None))
            if base and name:
                return str(Path(base) / name)
        return ""

    def __build_relative_media_path(self, target_path: str) -> str:
        normalized_target = self.__normalize_path(target_path)
        prefix = self._library_path_prefix
        if prefix and normalized_target.startswith(prefix.rstrip("/") + "/"):
            relative_path = normalized_target[len(prefix.rstrip("/")) + 1:]
            return self.__normalize_relative_path(relative_path)
        if prefix and normalized_target == prefix.rstrip("/"):
            return ""
        return self.__normalize_relative_path(normalized_target)

    def __find_pending_index(self, pending_items: List[Dict[str, Any]], target_path: str, relative_path: str) -> Optional[int]:
        for index, item in enumerate(pending_items):
            if target_path and item.get("target_path") == target_path:
                return index
            if relative_path and item.get("relative_path") == relative_path:
                return index
        return None

    def __find_matching_pending_indexes(self, pending_items: List[Dict[str, Any]], success_path: str) -> List[int]:
        success_path = self.__normalize_relative_path(success_path)
        success_name = Path(success_path).name
        path_matches = []
        name_matches = []

        for index, item in enumerate(pending_items):
            relative_path = self.__normalize_relative_path(item.get("relative_path") or "")
            filename = item.get("filename") or ""
            if relative_path and relative_path == success_path:
                path_matches.append(index)
            elif filename and filename == success_name:
                name_matches.append(index)

        if self._match_mode == self._MATCH_MODE_PATH_ONLY:
            return path_matches
        if self._match_mode == self._MATCH_MODE_NAME_ONLY:
            return name_matches if len(name_matches) == 1 else []
        if path_matches:
            return path_matches
        return name_matches if len(name_matches) == 1 else []

    @staticmethod
    def __trim_consumed_keys(consumed_keys: List[str]) -> List[str]:
        return sorted(consumed_keys)[-500:]

    @staticmethod
    def __safe_int(value: Any, default: int) -> int:
        try:
            return int(value)
        except Exception:
            return default

    @staticmethod
    def __path_to_string(value: Any) -> str:
        if not value:
            return ""
        if isinstance(value, Path):
            return str(value)
        return str(value).strip()

    @staticmethod
    def __normalize_path(value: str) -> str:
        return str(value or "").replace("\\", "/").rstrip("/")

    def __normalize_prefix(self, value: str) -> str:
        normalized = self.__normalize_path(value)
        return normalized.rstrip("/")

    def __normalize_relative_path(self, value: str) -> str:
        normalized = self.__normalize_path(value).lstrip("/")
        return normalized

    @staticmethod
    def __serialize_value(value: Any) -> Any:
        if value is None:
            return None
        if hasattr(value, "value"):
            return getattr(value, "value")
        return value

    @staticmethod
    def __parse_datetime(value: Optional[str]) -> Optional[datetime]:
        if not value:
            return None
        try:
            return datetime.fromisoformat(value)
        except Exception:
            return None

    def __update_config(self):
        self.update_config({
            "enabled": self._enabled,
            "debug": self._debug,
            "onlyonce": self._onlyonce,
            "delay": self._delay,
            "mediaservers": self._mediaservers,
            "rclone_log_path": self._rclone_log_path,
            "library_path_prefix": self._library_path_prefix,
            "log_poll_seconds": self._log_poll_seconds,
            "success_pattern": self._success_pattern,
            "match_mode": self._match_mode,
            "grace_delay": self._grace_delay,
            "pending_expire_minutes": self._pending_expire_minutes,
        })
