import copy
import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from app.core.event import eventmanager, Event
from app.log import logger
from app.plugins import _PluginBase
from app.schemas.types import EventType


class VarietyEpisodeRemap(_PluginBase):
    plugin_name = "综艺集数映射整理"
    plugin_desc = "按自定义映射修正综艺整理目标命名使用的季集编号，不修改源文件名。"
    plugin_icon = "video.png"
    plugin_version = "0.1"
    plugin_author = "Claude"
    author_url = "https://github.com/anthropics/claude-code"
    plugin_config_prefix = "varietyepisoderemap_"
    plugin_order = 31
    auth_level = 2

    _enabled: bool = False
    _debug: bool = False
    _dry_run: bool = True
    _fallback_mode: str = "pass_through"
    _test_filename: str = ""
    _run_test: bool = False
    _show_rules: List[Dict[str, Any]] = []

    _DEFAULT_SHOW_RULES: List[Dict[str, Any]] = [
        {
            "name": "乘风2023",
            "tmdb_id": None,
            "aliases": [
                "乘风2023",
                "乘风 2023",
                "乘风",
                "浪姐4",
                "浪姐 4",
                "乘风破浪的姐姐",
                "sisters.who.make.waves",
                "sisters who make waves",
            ],
            "rules": [
                {
                    "type": "preview",
                    "keywords": ["抢先看"],
                    "mapping": {
                        "S04E05": "S00E107",
                        "S04E07": "S00E116",
                        "S04E09": "S00E126",
                        "S04E11": "S00E135",
                    },
                },
                {
                    "type": "dorm",
                    "keywords": ["第六号宿舍"],
                    "mapping": {
                        "S04E01": "S00E089",
                        "S04E02": "S00E094",
                        "S04E03": "S00E098",
                        "S04E04": "S00E102",
                        "S04E05": "S00E106",
                        "S04E06": "S00E111",
                        "S04E07": "S00E115",
                        "S04E08": "S00E120",
                        "S04E09": "S00E124",
                        "S04E10": "S00E129",
                        "S04E11": "S00E134",
                        "S04E12": "S00E139",
                    },
                },
                {
                    "type": "stage",
                    "keywords": ["舞台纯享"],
                    "mapping": {
                        "S04E01": "S00E090",
                        "S04E02": "S00E095",
                        "S04E04": "S00E103",
                        "S04E06": "S00E112",
                        "S04E08": "S00E121",
                        "S04E10": "S00E130",
                        "S04E11": "S00E133",
                        "S04E12": "S00E138",
                    },
                },
                {
                    "type": "live",
                    "keywords": ["直播训练室"],
                    "mapping": {
                        "S04E01": "S00E091",
                        "S04E02": "S00E099",
                        "S04E03": "S00E108",
                        "S04E07": "S00E117",
                        "S04E09": "S00E125",
                    },
                },
                {
                    "type": "advance",
                    "keywords": ["超前营业"],
                    "mapping": {
                        "S04E02": "S00E093",
                        "S04E03": "S00E097",
                        "S04E04": "S00E101",
                        "S04E05": "S00E105",
                        "S04E07": "S00E110",
                        "S04E08": "S00E114",
                        "S04E09": "S00E123",
                        "S04E10": "S00E128",
                        "S04E11": "S00E132",
                        "S04E12": "S00E137",
                    },
                },
                {
                    "type": "plus",
                    "keywords": [".plus", "加更版", "会员加更", "plus"],
                    "mapping": {
                        "S04E01.Plus": "S00E092",
                        "S04E02.Plus": "S00E096",
                        "S04E03.Plus": "S00E100",
                        "S04E04.Plus": "S00E104",
                        "S04E05.Plus": "S00E109",
                        "S04E06.Plus": "S00E113",
                        "S04E07.Plus": "S00E118",
                        "S04E08.Plus": "S00E122",
                        "S04E09.Plus": "S00E127",
                        "S04E10.Plus": "S00E131",
                        "S04E11.Plus": "S00E136",
                        "S04E12.Plus": "S00E140",
                    },
                    "remove_tokens": ["Part1", "Part2", "Plus"],
                },
                {
                    "type": "main_part",
                    "keywords": [],
                    "mapping": {
                        "S04E01.Part1": "S04E01",
                        "S04E01.Part2": "S04E02",
                        "S04E02.Part1": "S04E03",
                        "S04E02.Part2": "S04E04",
                        "S04E03.Part1": "S04E05",
                        "S04E03.Part2": "S04E06",
                        "S04E04.Part1": "S04E07",
                        "S04E04.Part2": "S04E08",
                        "S04E06.Part1": "S04E10",
                        "S04E06.Part2": "S04E11",
                        "S04E08.Part1": "S04E13",
                        "S04E08.Part2": "S04E14",
                        "S04E10.Part1": "S04E16",
                        "S04E10.Part2": "S04E17",
                    },
                    "remove_tokens": ["Part1", "Part2", "Plus"],
                },
                {
                    "type": "main_single",
                    "keywords": [],
                    "mapping": {
                        "S04E05": "S04E09",
                        "S04E07": "S04E12",
                        "S04E09": "S04E15",
                        "S04E11": "S04E18",
                        "S04E12": "S04E19",
                    },
                },
            ],
        }
    ]

    def init_plugin(self, config: dict = None):
        if config:
            self._enabled = bool(config.get("enabled", False))
            self._debug = bool(config.get("debug", False))
            self._dry_run = bool(config.get("dry_run", True))
            self._fallback_mode = config.get("fallback_mode") or "pass_through"
            self._test_filename = config.get("test_filename", "")
            self._run_test = bool(config.get("run_test", False))
            self._show_rules = self.__load_show_rules(config.get("show_rules"))
        else:
            self._show_rules = copy.deepcopy(self._DEFAULT_SHOW_RULES)

        if self._run_test and self._test_filename:
            self.__run_test(self._test_filename)
            self._run_test = False
            self.__update_config()

    def get_state(self) -> bool:
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        return []

    def get_api(self) -> List[Dict[str, Any]]:
        return []

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        sample_rules = json.dumps(self._DEFAULT_SHOW_RULES, ensure_ascii=False, indent=2)
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
                                    "props": {"model": "dry_run", "label": "仅干跑不实改"}
                                }]
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [{
                                    "component": "VSwitch",
                                    "props": {"model": "run_test", "label": "执行测试（保存后运行）"}
                                }]
                            },
                        ]
                    },
                    {
                        "component": "VRow",
                        "content": [{
                            "component": "VCol",
                            "props": {"cols": 12, "md": 4},
                            "content": [{
                                "component": "VSelect",
                                "props": {
                                    "model": "fallback_mode",
                                    "label": "未命中规则时",
                                    "items": [
                                        {"title": "透传", "value": "pass_through"},
                                        {"title": "跳过并记日志", "value": "skip_when_unmapped"},
                                    ],
                                }
                            }]
                        }]
                    },
                    {
                        "component": "VRow",
                        "content": [{
                            "component": "VCol",
                            "props": {"cols": 12},
                            "content": [{
                                "component": "VTextField",
                                "props": {
                                    "model": "test_filename",
                                    "label": "测试文件名",
                                    "placeholder": "输入一个原始文件名，保存后查看日志中的映射结果",
                                }
                            }]
                        }]
                    },
                    {
                        "component": "VRow",
                        "content": [{
                            "component": "VCol",
                            "props": {"cols": 12},
                            "content": [{
                                "component": "VTextarea",
                                "props": {
                                    "model": "show_rules",
                                    "label": "节目映射规则 JSON",
                                    "rows": 18,
                                    "autoGrow": True,
                                    "placeholder": sample_rules,
                                }
                            }]
                        }]
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
                                    "text": "插件只尝试修正整理目标命名使用的季集映射，不会修改源文件名。首版内置乘风2023规则，推荐先开启干跑模式观察日志与 transferinfo 结构。"
                                }
                            }]
                        }]
                    }
                ]
            }
        ], {
            "enabled": False,
            "debug": False,
            "dry_run": True,
            "fallback_mode": "pass_through",
            "test_filename": "",
            "run_test": False,
            "show_rules": sample_rules,
        }

    def get_page(self) -> List[dict]:
        return []

    def stop_service(self):
        pass

    @eventmanager.register(EventType.TransferComplete)
    def handle_transfer_complete(self, event: Event = None):
        if not self._enabled or not event or not event.event_data:
            return

        transferinfo = event.event_data.get("transferinfo")
        if not transferinfo:
            return

        candidate_texts = self.__extract_candidate_texts(transferinfo)
        match = None
        matched_text = None
        for text in candidate_texts:
            match = self.__match_filename(text)
            if match:
                matched_text = text
                break

        if self._debug:
            logger.info("[VarietyEpisodeRemap] transferinfo candidate texts: %s", candidate_texts)
            logger.info("[VarietyEpisodeRemap] transferinfo attrs: %s", self.__safe_attr_names(transferinfo))

        if not match:
            if self._debug:
                logger.info("[VarietyEpisodeRemap] no remap match found for transfer")
            elif self._fallback_mode == "skip_when_unmapped":
                logger.warning("[VarietyEpisodeRemap] transfer skipped because no mapping rule matched")
            return

        logger.info(
            "[VarietyEpisodeRemap] matched show=%s rule=%s source=%s target=%s filename=%s",
            match["show_name"],
            match["rule_type"],
            match["source_code"],
            match["target_code"],
            matched_text,
        )

        if self._dry_run:
            logger.info("[VarietyEpisodeRemap] dry_run enabled, rewritten target would be: %s", match["rewritten_name"])
            return

        applied = self.__apply_transfer_override(transferinfo, match)
        if applied:
            logger.info("[VarietyEpisodeRemap] applied target remap to transferinfo")
        else:
            logger.warning("[VarietyEpisodeRemap] no writable target naming field found on transferinfo; keep dry_run on and inspect runtime object")

    def __run_test(self, filename: str):
        match = self.__match_filename(filename)
        if not match:
            logger.info("[VarietyEpisodeRemap] [test] 未命中任何规则：%s", filename)
            return
        logger.info(
            "[VarietyEpisodeRemap] [test] show=%s rule=%s source=%s target=%s rewritten=%s",
            match["show_name"],
            match["rule_type"],
            match["source_code"],
            match["target_code"],
            match["rewritten_name"],
        )

    def __match_filename(self, filename: str) -> Optional[Dict[str, Any]]:
        normalized = self.__normalize_text(filename)
        for show in self._show_rules:
            if not self.__match_show(normalized, show):
                continue
            for rule in show.get("rules") or []:
                if not self.__rule_keywords_match(normalized, rule):
                    continue
                source_code = self.__extract_source_code(filename, rule)
                if not source_code:
                    continue
                target_code = (rule.get("mapping") or {}).get(source_code)
                if not target_code:
                    continue
                rewritten_name = self.__rewrite_filename(filename, source_code, target_code, rule)
                if not rewritten_name:
                    continue
                return {
                    "show_name": show.get("name"),
                    "rule_type": rule.get("type"),
                    "source_code": source_code,
                    "target_code": target_code,
                    "rewritten_name": rewritten_name,
                }
        return None

    def __match_show(self, normalized: str, show: Dict[str, Any]) -> bool:
        aliases = show.get("aliases") or []
        return any(self.__normalize_text(alias) in normalized for alias in aliases)

    def __rule_keywords_match(self, normalized: str, rule: Dict[str, Any]) -> bool:
        keywords = rule.get("keywords") or []
        if not keywords:
            return True
        return any(self.__normalize_text(keyword) in normalized for keyword in keywords)

    def __extract_source_code(self, filename: str, rule: Dict[str, Any]) -> Optional[str]:
        se_match = re.search(r"S\d{2}E\d{2}", filename, re.IGNORECASE)
        if not se_match:
            return None
        code = se_match.group(0).upper()
        rule_type = rule.get("type")
        normalized = self.__normalize_text(filename)

        if rule_type == "plus":
            return f"{code}.Plus" if "plus" in normalized or "加更" in normalized else None

        if rule_type == "main_part":
            part_match = re.search(r"\.Part\s*([12])\b", filename, re.IGNORECASE)
            if not part_match:
                return None
            return f"{code}.Part{part_match.group(1)}"

        if rule_type == "main_single":
            if re.search(r"\.Part\s*[12]\b", filename, re.IGNORECASE):
                return None
            if "plus" in normalized or "加更" in normalized:
                return None
            return code

        return code

    def __rewrite_filename(self, filename: str, source_code: str, target_code: str, rule: Dict[str, Any]) -> Optional[str]:
        se_match = re.search(r"S\d{2}E\d{2}", filename, re.IGNORECASE)
        if not se_match:
            return None

        rewritten = filename[:se_match.start()] + target_code + filename[se_match.end():]
        for token in rule.get("remove_tokens") or []:
            rewritten = re.sub(rf"\.{re.escape(token)}\b", "", rewritten, flags=re.IGNORECASE)
        rewritten = re.sub(r"\.\.+", ".", rewritten)
        return rewritten

    def __apply_transfer_override(self, transferinfo: Any, match: Dict[str, Any]) -> bool:
        target_fields = [
            "target_name",
            "rename_name",
            "new_name",
            "dest_name",
            "target_file",
            "target_filename",
        ]
        for field in target_fields:
            if hasattr(transferinfo, field):
                try:
                    setattr(transferinfo, field, match["rewritten_name"])
                    return True
                except Exception as err:
                    logger.warning("[VarietyEpisodeRemap] failed setting %s: %s", field, err)
        return False

    def __extract_candidate_texts(self, transferinfo: Any) -> List[str]:
        candidates: List[str] = []
        fields = [
            "src",
            "src_path",
            "source_path",
            "in_path",
            "path",
            "file_path",
            "file_name",
            "filename",
            "name",
            "title",
            "target_name",
            "rename_name",
            "new_name",
            "dest_name",
        ]
        for field in fields:
            value = getattr(transferinfo, field, None)
            if not value:
                continue
            if isinstance(value, Path):
                value = str(value)
            if isinstance(value, str):
                candidates.append(value)
                candidates.append(Path(value).name)
        deduped: List[str] = []
        seen = set()
        for item in candidates:
            item = item.strip()
            if not item or item in seen:
                continue
            seen.add(item)
            deduped.append(item)
        return deduped

    @staticmethod
    def __safe_attr_names(obj: Any) -> List[str]:
        attrs = []
        for name in dir(obj):
            if name.startswith("_"):
                continue
            try:
                value = getattr(obj, name)
            except Exception:
                continue
            if callable(value):
                continue
            attrs.append(name)
        return attrs

    @staticmethod
    def __normalize_text(text: str) -> str:
        return re.sub(r"\s+", "", str(text or "")).lower()

    def __load_show_rules(self, raw_rules: Any) -> List[Dict[str, Any]]:
        if not raw_rules:
            return copy.deepcopy(self._DEFAULT_SHOW_RULES)
        if isinstance(raw_rules, list):
            return raw_rules
        if isinstance(raw_rules, str):
            try:
                parsed = json.loads(raw_rules)
                if isinstance(parsed, list) and parsed:
                    return parsed
            except Exception as err:
                logger.warning("[VarietyEpisodeRemap] show_rules JSON parse failed, fallback to defaults: %s", err)
        return copy.deepcopy(self._DEFAULT_SHOW_RULES)

    def __update_config(self):
        self.update_config({
            "enabled": self._enabled,
            "debug": self._debug,
            "dry_run": self._dry_run,
            "fallback_mode": self._fallback_mode,
            "test_filename": self._test_filename,
            "run_test": self._run_test,
            "show_rules": json.dumps(self._show_rules, ensure_ascii=False, indent=2),
        })
