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
    plugin_desc = "输入种子标题，识别媒体分类，测试能命中对应分类规则组的哪个优先级档位"
    # 插件图标
    plugin_icon = "filter.png"
    # 插件版本
    plugin_version = "1.2"
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

    def __run_filter_test(self):
        title = self._torrent_title.strip()
        desc = self._torrent_desc.strip()
        logger.info(f"[FilterTest] ===== 开始测试种子优先级档位 =====")
        logger.info(f"[FilterTest] 种子标题：{title}")
        if desc:
            logger.info(f"[FilterTest] 种子描述：{desc}")

        # 识别媒体信息
        meta = MetaInfo(title=title, subtitle=desc)
        mediainfo: Optional[MediaInfo] = self.chain.recognize_media(meta=meta)
        if mediainfo:
            logger.info(f"[FilterTest] 识别媒体：{mediainfo.title}，类型：{mediainfo.type}，二级分类：{mediainfo.category or '未识别'}")
        else:
            logger.info(f"[FilterTest] 未识别到媒体信息")

        category = (mediainfo.category if mediainfo else None) or ""

        # 获取所有规则组
        all_groups: list = self.systemconfig.get(SystemConfigKey.UserFilterRuleGroups) or []
        if not all_groups:
            logger.warning(f"[FilterTest] 未找到任何规则组，请先在 设定→规则 中配置")
            return

        # 找到与媒体分类匹配的规则组
        matched_group = None
        for group in all_groups:
            if not isinstance(group, dict):
                continue
            group_category = str(group.get("category") or "").strip()
            if category and group_category == category:
                matched_group = group
                break

        if matched_group:
            logger.info(f"[FilterTest] 找到匹配分类 [{category}] 的规则组：{matched_group.get('name', '')}")
        else:
            logger.warning(f"[FilterTest] 未找到匹配分类 [{category}] 的规则组，将测试所有规则组")

        # 构造 TorrentInfo
        torrentinfo = TorrentInfo(title=title, description=desc)

        if matched_group:
            self.__test_group_tiers(torrentinfo, mediainfo, matched_group)
        else:
            for group in all_groups:
                if not isinstance(group, dict):
                    continue
                self.__test_group_tiers(torrentinfo, mediainfo, group)

        logger.info(f"[FilterTest] ===== 测试结束 =====")

    def __test_group_tiers(self, torrentinfo: TorrentInfo, mediainfo: Optional[MediaInfo], group: dict):
        group_name = group.get("name", "未命名规则组")
        rule_string = str(group.get("rule_string") or "").strip()
        if not rule_string:
            logger.info(f"[FilterTest] 规则组 [{group_name}] 无规则字符串，跳过")
            return

        tiers = [t.strip() for t in rule_string.split(">") if t.strip()]
        logger.info(f"[FilterTest] 规则组 [{group_name}] 共 {len(tiers)} 个档位")

        hit_tier = None
        for i, tier in enumerate(tiers):
            test_group = dict(group)
            test_group["rule_string"] = tier
            try:
                result = self.chain.filter_torrents(
                    rule_groups=[test_group],
                    torrent_list=[torrentinfo],
                    mediainfo=mediainfo
                )
                if result:
                    hit_tier = i + 1
                    logger.info(f"[FilterTest] ✓ 命中档位 {i+1}（{tier}）")
                    break
                else:
                    logger.info(f"[FilterTest]   档位 {i+1} 未命中（{tier}）")
            except Exception as e:
                logger.error(f"[FilterTest] 测试档位 {i+1} 出错：{e}")

        if hit_tier:
            logger.info(f"[FilterTest] ★ 结论：种子命中规则组 [{group_name}] 第 {hit_tier} 档位")
            logger.info(f"[FilterTest]   即时推送档位设置 <= {hit_tier} 时可触发即时推送")
        else:
            logger.warning(f"[FilterTest] ✗ 种子未命中规则组 [{group_name}] 的任何档位")

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
                        'content': [{
                            'component': 'VCol',
                            'props': {'cols': 12, 'md': 4},
                            'content': [{
                                'component': 'VSwitch',
                                'props': {'model': 'enabled', 'label': '启用插件'}
                            }]
                        }]
                    },
                    {
                        'component': 'VRow',
                        'content': [{
                            'component': 'VCol',
                            'props': {'cols': 12},
                            'content': [{
                                'component': 'VTextField',
                                'props': {
                                    'model': 'torrent_title',
                                    'label': '种子标题',
                                    'placeholder': '粘贴种子标题',
                                }
                            }]
                        }]
                    },
                    {
                        'component': 'VRow',
                        'content': [{
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
                        }]
                    },
                    {
                        'component': 'VRow',
                        'content': [{
                            'component': 'VCol',
                            'props': {'cols': 12, 'md': 4},
                            'content': [{
                                'component': 'VSwitch',
                                'props': {'model': 'run_test', 'label': '执行测试（保存后立即运行）'}
                            }]
                        }]
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
                                    'text': '使用方法：输入种子标题 → 打开"执行测试"开关 → 保存。结果输出到插件日志。'
                                         '插件会自动识别媒体分类，找到对应规则组，逐档测试种子能命中哪个档位。'
                                         '档位数字越小优先级越高（第1档最高）。即时推送档位设置<=命中档位时可触发即时推送。',
                                }
                            }]
                        }]
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
