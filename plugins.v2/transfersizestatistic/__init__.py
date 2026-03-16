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
    plugin_desc = "统计已整理文件的总大小，仪表盘展示多时间范围数据，支持阈值通知。"
    # 插件图标
    plugin_icon = "statistic.png"
    # 插件版本
    plugin_version = "2.1"
    # 插件作者
    plugin_author = "jager"
    # 作者主页
    author_url = "https://github.com/jagernb/MoviePilot-Plugins"
    # 插件配置项ID前缀
    plugin_config_prefix = "transfersizestatistic_"
    # 加载顺序
    plugin_order = 30
    # 可使用的用户级别
    auth_level = 2

    # 私有属性
    _scheduler = None
    _enabled = False
    _onlyonce = False
    _notify = False
    _cron = None
    _threshold_enabled = False
    _threshold_gb = 0
    _clear_data = False

    def init_plugin(self, config: dict = None):
        if config:
            self._enabled = config.get("enabled", False)
            self._onlyonce = config.get("onlyonce", False)
            self._notify = config.get("notify", False)
            self._cron = config.get("cron")
            self._threshold_enabled = config.get("threshold_enabled", False)
            self._threshold_gb = float(config.get("threshold_gb") or 0)
            self._clear_data = config.get("clear_data", False)

        # 清空历史数据
        if self._clear_data:
            self.save_data("transfer_records", [])
            logger.info("已清空整理文件大小历史记录")
            self._clear_data = False
            self.update_config({
                "enabled": self._enabled,
                "onlyonce": self._onlyonce,
                "notify": self._notify,
                "cron": self._cron,
                "threshold_enabled": self._threshold_enabled,
                "threshold_gb": self._threshold_gb,
                "clear_data": False,
            })

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
                "threshold_enabled": self._threshold_enabled,
                "threshold_gb": self._threshold_gb,
                "clear_data": False,
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
                                'props': {'cols': 12, 'md': 3},
                                'content': [{
                                    'component': 'VSwitch',
                                    'props': {'model': 'enabled', 'label': '启用插件'}
                                }]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 3},
                                'content': [{
                                    'component': 'VSwitch',
                                    'props': {'model': 'notify', 'label': '发送通知'}
                                }]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 3},
                                'content': [{
                                    'component': 'VSwitch',
                                    'props': {'model': 'threshold_enabled', 'label': '启用24h阈值通知'}
                                }]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 3},
                                'content': [{
                                    'component': 'VSwitch',
                                    'props': {'model': 'onlyonce', 'label': '立即运行一次'}
                                }]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 3},
                                'content': [{
                                    'component': 'VSwitch',
                                    'props': {'model': 'clear_data', 'label': '清空历史数据'}
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
                                        'label': '通知周期',
                                        'placeholder': '5位cron表达式，如 0 8 * * *'
                                    }
                                }]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 6},
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
                                    'text': '仪表盘直接展示4种统计数据（滚动24小时/今日/7天/30天）。'
                                            '通知周期控制定时发送通知的频率，留空则不定时通知。'
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
            "threshold_enabled": False,
            "threshold_gb": 0,
            "clear_data": False,
        }

    def get_page(self) -> List[dict]:
        pass

    def get_dashboard(self, key: str, **kwargs) -> Optional[Tuple[Dict[str, Any], Dict[str, Any], List[dict]]]:
        """
        获取插件仪表盘页面
        """
        cols = {"cols": 12}
        attrs = {"refresh": 300}
        records = self.get_data("transfer_records") or []
        stats = {
            "24h": ("滚动24小时", "mdi-hours-24"),
            "today": ("今日（自然日）", "mdi-calendar-today"),
            "7d": ("最近7天", "mdi-calendar-week"),
            "30d": ("最近30天", "mdi-calendar-month"),
        }
        card_elements = []
        for rk, (label, icon) in stats.items():
            total = self._calc_size(records, rk)
            gb = total / 1073741824
            card_elements.append({
                'component': 'VCol',
                'props': {'cols': 6, 'md': 3},
                'content': [{
                    'component': 'VCard',
                    'props': {'variant': 'tonal'},
                    'content': [{
                        'component': 'VCardText',
                        'props': {'class': 'd-flex align-center'},
                        'content': [
                            {
                                'component': 'VAvatar',
                                'props': {
                                    'rounded': True,
                                    'variant': 'text',
                                    'class': 'me-3'
                                },
                                'content': [{
                                    'component': 'VIcon',
                                    'props': {'icon': icon}
                                }]
                            },
                            {
                                'component': 'div',
                                'content': [
                                    {
                                        'component': 'span',
                                        'props': {'class': 'text-caption'},
                                        'text': label
                                    },
                                    {
                                        'component': 'div',
                                        'props': {'class': 'd-flex align-center flex-wrap'},
                                        'content': [{
                                            'component': 'span',
                                            'props': {'class': 'text-h6'},
                                            'text': f'{gb:.2f} GB'
                                        }]
                                    }
                                ]
                            }
                        ]
                    }]
                }]
            })
        elements = [{'component': 'VRow', 'content': card_elements}]
        return cols, attrs, elements

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
        # total_size 是整个批量任务的总大小，需要除以文件数得到单文件大小
        file_count = getattr(transferinfo, "file_count", 0) or 0
        if file_count > 1:
            file_size = total_size / file_count
        else:
            file_size = total_size

        with lock:
            now_str = datetime.now(tz=pytz.timezone(settings.TZ)).isoformat()
            records = self.get_data("transfer_records") or []
            records.append({"timestamp": now_str, "size": file_size})
            # 清理超过30天的旧记录
            cutoff_30d = (datetime.now(tz=pytz.timezone(settings.TZ)) - timedelta(days=30)).isoformat()
            records = [r for r in records if r.get("timestamp", "") > cutoff_30d]
            self.save_data("transfer_records", records)

        gb = file_size / 1073741824
        logger.info(f"记录整理文件大小: {gb:.2f} GB")

        # 阈值检查
        if self._threshold_enabled and self._threshold_gb > 0:
            size_24h = self._calc_size(records, "24h")
            size_24h_gb = size_24h / 1073741824
            if size_24h_gb >= self._threshold_gb:
                last_notify = self.get_data("last_threshold_notify") or ""
                today_str = datetime.now(tz=pytz.timezone(settings.TZ)).strftime("%Y-%m-%d-%H")
                if last_notify != today_str:
                    self.save_data("last_threshold_notify", today_str)
                    self.post_message(
                        mtype=NotificationType.SiteMessage,
                        title="【整理文件大小统计 - 阈值告警】",
                        text=f"滚动24小时内整理文件总大小已达 {size_24h_gb:.2f} GB，"
                             f"超过阈值 {self._threshold_gb} GB"
                    )

    def _calc_size(self, records: list, range_key: str) -> int:
        """计算指定范围内的文件总大小(bytes)"""
        now = datetime.now(tz=pytz.timezone(settings.TZ))
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
        records = self.get_data("transfer_records") or []
        range_labels = [
            ("24h", "滚动24小时"),
            ("today", "今日（自然日）"),
            ("7d", "最近7天"),
            ("30d", "最近30天"),
        ]
        lines = []
        for rk, label in range_labels:
            total = self._calc_size(records, rk)
            gb = total / 1073741824
            lines.append(f"{label}: {gb:.2f} GB")

        text = "\n".join(lines)
        logger.info(f"整理文件大小统计:\n{text}")

        if self._notify:
            self.post_message(
                mtype=NotificationType.SiteMessage,
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
