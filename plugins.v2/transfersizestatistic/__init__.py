import threading
from datetime import datetime, timedelta
from typing import List, Tuple, Dict, Any, Optional

import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from app.core.config import settings
from app.core.event import eventmanager, Event
from app.log import logger
from app.plugins import _PluginBase
from app.schemas import NotificationType
from app.schemas.types import EventType

lock = threading.Lock()


class TransferSizeStatistic(_PluginBase):
    # 插件名称
    plugin_name = "整理文件大小统计"
    # 插件描述
    plugin_desc = "统计已整理文件的总大小，支持多时间范围统计和阈值通知。"
    # 插件图标
    plugin_icon = "statistic.png"
    # 插件版本
    plugin_version = "1.0"
    # 插件作者
    plugin_author = "jager"
    # 作者主页
    author_url = "https://github.com/jagernb/MoviePilot-Plugins"
    # 插件配置项ID前缀
    plugin_config_prefix = "transfersizestatistic_"
    # 加载顺序
    plugin_order = 30
    # 可使用的用户级别
    auth_level = 1

    # 私有属性
    _scheduler = None
    _enabled = False
    _onlyonce = False
    _notify = False
    _cron = None
    _time_ranges = []
    _threshold_enabled = False
    _threshold_gb = 0
    def init_plugin(self, config: dict = None):
        if config:
            self._enabled = config.get("enabled", False)
            self._onlyonce = config.get("onlyonce", False)
            self._notify = config.get("notify", False)
            self._cron = config.get("cron")
            self._time_ranges = config.get("time_ranges") or []
            self._threshold_enabled = config.get("threshold_enabled", False)
            self._threshold_gb = float(config.get("threshold_gb") or 0)

        self.stop_service()

        if self._onlyonce:
            self._scheduler = BackgroundScheduler(timezone=settings.TZ)
            logger.info("整理文件大小统计服务启动，立即运行一次")
            self._scheduler.add_job(
                func=self.__report,
                trigger='date',
                run_date=datetime.now(tz=pytz.timezone(settings.TZ)) + timedelta(seconds=3)
            )
            self._onlyonce = False
            self.update_config({
                "enabled": self._enabled,
                "onlyonce": False,
                "notify": self._notify,
                "cron": self._cron,
                "time_ranges": self._time_ranges,
                "threshold_enabled": self._threshold_enabled,
                "threshold_gb": self._threshold_gb,
            })
            if self._scheduler.get_jobs():
                self._scheduler.print_jobs()
                self._scheduler.start()

    def get_state(self) -> bool:
        return True if self._enabled else False

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        pass

    def get_api(self) -> List[Dict[str, Any]]:
        pass

    def get_service(self) -> List[Dict[str, Any]]:
        if self._enabled and self._cron:
            return [{
                "id": "TransferSizeStatistic",
                "name": "整理文件大小统计服务",
                "trigger": CronTrigger.from_crontab(self._cron),
                "func": self.__report,
                "kwargs": {}
            }]
        return []
    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        return [
            {
                'component': 'VForm',
                'content': [
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 4},
                                'content': [{
                                    'component': 'VSwitch',
                                    'props': {'model': 'enabled', 'label': '启用插件'}
                                }]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 4},
                                'content': [{
                                    'component': 'VSwitch',
                                    'props': {'model': 'notify', 'label': '发送通知'}
                                }]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 4},
                                'content': [{
                                    'component': 'VSwitch',
                                    'props': {'model': 'onlyonce', 'label': '立即运行一次'}
                                }]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 6},
                                'content': [{
                                    'component': 'VTextField',
                                    'props': {
                                        'model': 'cron',
                                        'label': '执行周期',
                                        'placeholder': '5位cron表达式，如 0 8 * * *'
                                    }
                                }]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 6},
                                'content': [{
                                    'component': 'VSelect',
                                    'props': {
                                        'model': 'time_ranges',
                                        'label': '统计范围',
                                        'multiple': True,
                                        'chips': True,
                                        'items': [
                                            {'title': '滚动24小时', 'value': '24h'},
                                            {'title': '今日（自然日）', 'value': 'today'},
                                            {'title': '最近7天', 'value': '7d'},
                                            {'title': '最近30天', 'value': '30d'}
                                        ]
                                    }
                                }]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 4},
                                'content': [{
                                    'component': 'VSwitch',
                                    'props': {
                                        'model': 'threshold_enabled',
                                        'label': '启用24h阈值通知'
                                    }
                                }]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 8},
                                'content': [{
                                    'component': 'VTextField',
                                    'props': {
                                        'model': 'threshold_gb',
                                        'label': '24小时滚动阈值(GB)',
                                        'placeholder': '如 100，达到后立即通知'
                                    }
                                }]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [{
                            'component': 'VCol',
                            'props': {'cols': 12},
                            'content': [{
                                'component': 'VAlert',
                                'props': {
                                    'type': 'info',
                                    'variant': 'tonal',
                                    'text': '注意：本插件只能统计启用后的整理记录，'
                                            '通过监听整理完成事件来记录文件大小。'
                                }
                            }]
                        }]
                    }
                ]
            }
        ], {
            "enabled": False,
            "onlyonce": False,
            "notify": False,
            "cron": "",
            "time_ranges": ["24h"],
            "threshold_enabled": False,
            "threshold_gb": 0,
        }

    def get_page(self) -> List[dict]:
        pass
    @eventmanager.register(EventType.TransferComplete)
    def handle_transfer_complete(self, event: Event = None):
        """监听整理完成事件，记录文件大小"""
        if not self._enabled:
            return
        if not event or not event.event_data:
            return
        transferinfo = event.event_data.get("transferinfo")
        if not transferinfo:
            return
        total_size = getattr(transferinfo, "total_size", 0) or 0
        if total_size <= 0:
            return

        with lock:
            now_str = datetime.now(tz=pytz.timezone(settings.TZ)).isoformat()
            records = self.get_data("transfer_records") or []
            records.append({"timestamp": now_str, "size": total_size})
            # 清理超过30天的旧记录
            cutoff = (datetime.now(tz=pytz.timezone(settings.TZ)) - timedelta(days=31)).isoformat()
            records = [r for r in records if r.get("timestamp", "") > cutoff]
            self.save_data("transfer_records", records)

        logger.info(f"记录整理文件大小: {total_size / 1073741824:.2f} GB")

        # 检查24h滚动阈值
        if self._threshold_enabled and self._threshold_gb > 0:
            size_24h = self._calc_size(records, "24h")
            size_24h_gb = size_24h / 1073741824
            if size_24h_gb >= self._threshold_gb:
                # 防止重复通知：检查是否已通知过
                last_notify = self.get_data("last_threshold_notify") or ""
                today_str = datetime.now(tz=pytz.timezone(settings.TZ)).strftime("%Y-%m-%d-%H")
                if last_notify != today_str:
                    self.save_data("last_threshold_notify", today_str)
                    self.post_message(
                        mtype=NotificationType.Plugin,
                        title="【整理文件大小统计 - 阈值告警】",
                        text=f"滚动24小时内整理文件总大小已达 {size_24h_gb:.2f} GB，"
                             f"超过阈值 {self._threshold_gb} GB"
                    )

    def _calc_size(self, records: list, range_key: str) -> int:
        """计算指定时间范围内的总大小（字节）"""
        tz = pytz.timezone(settings.TZ)
        now = datetime.now(tz=tz)
        if range_key == "24h":
            cutoff = (now - timedelta(hours=24)).isoformat()
        elif range_key == "today":
            cutoff = now.replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
        elif range_key == "7d":
            cutoff = (now - timedelta(days=7)).isoformat()
        elif range_key == "30d":
            cutoff = (now - timedelta(days=30)).isoformat()
        else:
            return 0
        return sum(r.get("size", 0) for r in records if r.get("timestamp", "") > cutoff)

    def __report(self):
        """定时汇总统计并通知"""
        if not self._time_ranges:
            logger.warning("未选择统计范围，跳过")
            return
        records = self.get_data("transfer_records") or []
        range_labels = {
            "24h": "滚动24小时",
            "today": "今日（自然日）",
            "7d": "最近7天",
            "30d": "最近30天"
        }
        lines = []
        for rk in self._time_ranges:
            total = self._calc_size(records, rk)
            gb = total / 1073741824
            label = range_labels.get(rk, rk)
            lines.append(f"{label}: {gb:.2f} GB")

        text = "\n".join(lines)
        logger.info(f"整理文件大小统计:\n{text}")

        if self._notify:
            self.post_message(
                mtype=NotificationType.Plugin,
                title="【整理文件大小统计】",
                text=text
            )

    def stop_service(self):
        try:
            if self._scheduler:
                self._scheduler.remove_all_jobs()
                if self._scheduler.running:
                    self._scheduler.shutdown()
                self._scheduler = None
        except Exception as e:
            logger.error(f"退出插件失败：{e}")
