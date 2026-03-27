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
    plugin_desc = "输入种子标题，测试能命中订阅过滤规则的哪个优先级（pri_order）"
    # 插件图标
    plugin_icon = "filter.png"
    # 插件版本
    plugin_version = "1.0"
    # 插件作者
    plugin_author = "jager"
    # 作者主页
    author_url = "https://github.com/jagernb/MoviePilot-Plugins"
    # 插件配置项ID前缀
    plugin_config_prefix = "torrentfiltertest_"
    # 加载顺序
    plugin_order = 99
    # 可使用的用户级别
    auth_level = 1

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
                # 重置按钮状态
                config["run_test"] = False
                self.update_config(config)

    def __run_filter_test(self):
        title = self._torrent_title.strip()
        desc = self._torrent_desc.strip()
        logger.info(f"[FilterTest] ===== 开始测试种子规则优先级 =====")
        logger.info(f"[FilterTest] 种子标题：{title}")
        if desc:
            logger.info(f"[FilterTest] 种子描述：{desc}")

        # 获取订阅过滤规则组
        filter_groups = self.systemconfig.get(SystemConfigKey.SubscribeFilterRuleGroups)
        if not filter_groups:
            logger.warning(f"[FilterTest] 未找到订阅过滤规则组（设定→规则→订阅过滤规则），请先配置规则")
            return

        logger.info(f"[FilterTest] 共找到 {len(filter_groups)} 个规则组")
        for i, group in enumerate(filter_groups):
            name = group.get("name", f"规则组{i+1}") if isinstance(group, dict) else f"规则组{i+1}"
            logger.info(f"[FilterTest]   规则组 {i+1}：{name}")

        # 构造 TorrentInfo
        torrentinfo = TorrentInfo(
            title=title,
            description=desc,
        )

        # 识别媒体信息
        meta = MetaInfo(title=title, subtitle=desc)
        mediainfo: Optional[MediaInfo] = self.chain.recognize_media(meta=meta)
        if mediainfo:
            logger.info(f"[FilterTest] 识别媒体：{mediainfo.title}，类型：{mediainfo.type}，分类：{mediainfo.category}")
        else:
            logger.info(f"[FilterTest] 未识别到媒体信息，仅按种子标题匹配规则")

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
                # 计算命中的是第几个规则组
                matched_index = 100 - pri_order  # 第1组=100，第2组=99...
                if matched_index <= 0:
                    group_info = "第1个规则组（最高优先级）"
                elif matched_index < len(filter_groups):
                    group_info = f"第{matched_index + 1}个规则组"
                else:
                    group_info = f"规则组索引 {matched_index}"

                logger.info(f"[FilterTest] ✓ 命中规则！pri_order = {pri_order}")
                logger.info(f"[FilterTest] ✓ 对应：{group_info}（pri_order=100表示第1组，99表示第2组，以此类推）")

                # 即时推送阈值说明
                for threshold in [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14]:
                    required = 101 - threshold
                    if pri_order >= required:
                        logger.info(f"[FilterTest]   → 即时推送档位 {threshold} 可触发（阈值 pri_order>={required}）")
                    else:
                        logger.info(f"[FilterTest]   → 即时推送档位 {threshold} 不触发（阈值 pri_order>={required}，当前{pri_order}不足）")
                        break
            else:
                logger.warning(f"[FilterTest] ✗ 未命中任何规则组，pri_order=0")
                logger.warning(f"[FilterTest]   该种子不会触发即时推送，请检查规则组配置是否覆盖此种子")
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
                                        'text': '使用方法：输入种子标题 → 打开"执行测试"开关 → 点击保存。结果将输出到插件日志中。'
                                             'pri_order=100 表示命中第1个规则组（最高优先级），99 表示第2个，以此类推。pri_order=0 表示未命中任何规则组。',
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
