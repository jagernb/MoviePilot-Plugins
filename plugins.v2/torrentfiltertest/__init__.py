from typing import Any, List, Dict, Tuple, Optional

from app.core.context import TorrentInfo, MediaInfo
from app.core.metainfo import MetaInfo
from app.log import logger
from app.plugins import _PluginBase
from app.schemas.types import SystemConfigKey


class TorrentFilterTest(_PluginBase):
    # 插件名称
    plugin_name = "种子规则优先级测试"
    # 插件描述
    plugin_desc = "输入种子标题，测试能命中订阅过滤规则的哪个优先级（pri_order），并查询RssSubscribe对应分类的即时推送阈值"
    # 插件图标
    plugin_icon = "filter.png"
    # 插件版本
    plugin_version = "1.1"
    # 插件作者
    plugin_author = "jager"
    # 作者主页
    author_url = "https://github.com/jagernb/MoviePilot-Plugins"
    # 插件配置项ID前缀
    plugin_config_prefix = "torrentfiltertest_"
    # 加载顺序
    plugin_order = 99
    # 可使用的用户级别
    auth_level = 2

    # 配置属性
    _enabled: bool = False
    _torrent_title: str = ""
    _torrent_desc: str = ""
    _run_test: bool = False

    def init_plugin(self, config: dict = None):
        if config:
            self._enabled = config.get("enabled", False)
            self._torrent_title = config.get("torrent_title", "")
            self._torrent_desc = config.get("torrent_desc", "")
            self._run_test = config.get("run_test", False)

            if self._run_test and self._torrent_title:
                self.__run_filter_test()
                config["run_test"] = False
                self.update_config(config)

    def __get_rsssubscribe_category_priority(self, category: str) -> int:
        """从 RssSubscribe 插件配置中读取指定分类的即时推送优先级"""
        try:
            rss_config = self.get_config("RssSubscribeJager") or self.get_config("rsssubscribe_jager") or {}
            # 尝试读取分类map
            category_map = rss_config.get("category_instant_priority_map") or {}
            if not isinstance(category_map, dict):
                import json
                try:
                    category_map = json.loads(category_map)
                except Exception:
                    category_map = {}
            # 标准化分类名后匹配
            normalized_category = category.strip().lower()
            for k, v in category_map.items():
                if k.strip().lower() == normalized_category:
                    return max(0, int(v or 0))
            # 回退到全局 instant_priority
            global_priority = int(rss_config.get("instant_priority") or 0)
            return global_priority
        except Exception as e:
            logger.debug(f"[FilterTest] 读取RssSubscribe配置失败：{e}")
            return 0

    def __run_filter_test(self):
        title = self._torrent_title.strip()
        desc = self._torrent_desc.strip()
        logger.info(f"[FilterTest] ===== 开始测试种子规则优先级 =====")
        logger.info(f"[FilterTest] 种子标题：{title}")
        if desc:
            logger.info(f"[FilterTest] 种子描述：{desc}")

        # 识别媒体信息
        meta = MetaInfo(title=title, subtitle=desc)
        mediainfo: Optional[MediaInfo] = self.chain.recognize_media(meta=meta)
        if mediainfo:
            logger.info(f"[FilterTest] 识别媒体：{mediainfo.title}，类型：{mediainfo.type}，二级分类：{mediainfo.category or '未识别'}")
        else:
            logger.info(f"[FilterTest] 未识别到媒体信息，仅按种子标题匹配规则")

        # 查询 RssSubscribe 中该分类对应的即时推送优先级
        category = (mediainfo.category if mediainfo else None) or ""
        instant_priority = self.__get_rsssubscribe_category_priority(category)
        if instant_priority > 0:
            threshold = 101 - instant_priority
            logger.info(f"[FilterTest] RssSubscribe分类 [{category or '全局'}] 即时推送档位：{instant_priority}，要求 pri_order >= {threshold}")
        else:
            logger.info(f"[FilterTest] RssSubscribe分类 [{category or '全局'}] 未配置即时推送优先级（或档位=0）")

        # 获取订阅过滤规则组（优先用 UserFilterRuleGroups，再试 SubscribeFilterRuleGroups）
        filter_groups = self.systemconfig.get(SystemConfigKey.UserFilterRuleGroups)
        if not filter_groups:
            try:
                filter_groups = self.systemconfig.get(SystemConfigKey.SubscribeFilterRuleGroups)
            except Exception:
                filter_groups = None
        if not filter_groups:
            logger.warning(f"[FilterTest] 未找到过滤规则组，请先在 设定→规则 中配置规则")
            return

        logger.info(f"[FilterTest] 共找到 {len(filter_groups)} 个规则组：")
        for i, group in enumerate(filter_groups):
            name = group.get("name", f"规则组{i+1}") if isinstance(group, dict) else f"规则组{i+1}"
            logger.info(f"[FilterTest]   [{i+1}] {name}")

        # 构造 TorrentInfo
        torrentinfo = TorrentInfo(
            title=title,
            description=desc,
        )

        # 调用过滤链
        try:
            filter_result = self.chain.filter_torrents(
                rule_groups=filter_groups,
                torrent_list=[torrentinfo],
                mediainfo=mediainfo
            )
            if filter_result:
                result_torrent = filter_result[0]
                pri_order = result_torrent.pri_order
                matched_index = 100 - pri_order  # 第1组=100，第2组=99...
                if 0 <= matched_index < len(filter_groups):
                    group = filter_groups[matched_index]
                    group_name = group.get("name", f"规则组{matched_index+1}") if isinstance(group, dict) else f"规则组{matched_index+1}"
                    group_info = f"第{matched_index+1}个规则组：{group_name}"
                else:
                    group_info = f"规则组索引 {matched_index}"

                logger.info(f"[FilterTest] ✓ 命中规则！pri_order = {pri_order}，对应 {group_info}")

                # 即时推送判断
                if instant_priority > 0:
                    threshold = 101 - instant_priority
                    if pri_order >= threshold:
                        logger.info(f"[FilterTest] ✓ 可触发即时推送（pri_order={pri_order} >= 阈值{threshold}，档位{instant_priority}）")
                    else:
                        logger.warning(f"[FilterTest] ✗ 不触发即时推送（pri_order={pri_order} < 阈值{threshold}，需提高规则组排名或调低即时推送档位）")
                else:
                    logger.info(f"[FilterTest] 未设置即时推送档位，跳过即时推送判断")
            else:
                logger.warning(f"[FilterTest] ✗ 未命中任何规则组，pri_order=0")
                logger.warning(f"[FilterTest]   该种子不会触发即时推送，请检查规则组是否能匹配此种子标题")
                if instant_priority > 0:
                    logger.warning(f"[FilterTest]   当前分类即时推送档位={instant_priority}，要求 pri_order>={101-instant_priority}，但种子未匹配规则组")
        except Exception as e:
            logger.error(f"[FilterTest] 调用 filter_torrents 出错：{e}")
            import traceback
            logger.error(traceback.format_exc())

        logger.info(f"[FilterTest] ===== 测试结束 =====")

    def get_state(self) -> bool:
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        return []

    def get_api(self) -> List[Dict[str, Any]]:
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
                                    'props': {
                                        'model': 'enabled',
                                        'label': '启用插件',
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
                                'props': {'cols': 12},
                                'content': [{
                                    'component': 'VTextField',
                                    'props': {
                                        'model': 'torrent_title',
                                        'label': '种子标题',
                                        'placeholder': '粘贴种子标题，如：[ANi] 某动漫 - 01 [1080P][Baha][WEB-DL][AAC AVC][CHT].mp4',
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
                                'props': {'cols': 12},
                                'content': [{
                                    'component': 'VTextField',
                                    'props': {
                                        'model': 'torrent_desc',
                                        'label': '种子描述（可选）',
                                        'placeholder': '可选，种子的副标题/描述信息',
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
                                        'model': 'run_test',
                                        'label': '执行测试（保存后立即运行）',
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
                                'props': {'cols': 12},
                                'content': [{
                                    'component': 'VAlert',
                                    'props': {
                                        'type': 'info',
                                        'variant': 'tonal',
                                        'text': '使用方法：输入种子标题 → 打开"执行测试"开关 → 点击保存。结果输出到插件日志。'
                                             'pri_order=100表示命中第1个规则组，99表示第2个，以此类推。pri_order=0表示未命中任何规则组。',
                                    }
                                }]
                            }
                        ]
                    }
                ]
            }
        ], {
            'enabled': False,
            'torrent_title': '',
            'torrent_desc': '',
            'run_test': False,
        }

    def get_page(self) -> List[dict]:
        return []

    def stop_service(self):
        pass
