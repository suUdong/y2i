from __future__ import annotations

import re

# ── Korean company name → (ticker, English name) ──────────────────────
# Suffix convention: .KS = KOSPI, .KQ = KOSDAQ

COMPANY_MAP: dict[str, tuple[str, str]] = {
    # ── Semiconductors / HBM / AI ──
    "엔비디아": ("NVDA", "NVIDIA"),
    "nvidia": ("NVDA", "NVIDIA"),
    "삼성전자": ("005930.KS", "Samsung Electronics"),
    "삼성": ("005930.KS", "Samsung Electronics"),
    "samsung": ("005930.KS", "Samsung Electronics"),
    "sk하이닉스": ("000660.KS", "SK hynix"),
    "하이닉스": ("000660.KS", "SK hynix"),
    "sk hynix": ("000660.KS", "SK hynix"),
    "마이크론": ("MU", "Micron"),
    "micron": ("MU", "Micron"),
    "브로드컴": ("AVGO", "Broadcom"),
    "broadcom": ("AVGO", "Broadcom"),
    "marvell": ("MRVL", "Marvell"),
    "마벨": ("MRVL", "Marvell"),
    "한미반도체": ("042700.KS", "Hanmi Semiconductor"),
    "리노공업": ("058470.KQ", "LEENO Industrial"),
    "가온칩스": ("399720.KQ", "Gaonchips"),
    "삼성전기": ("009150.KS", "Samsung Electro-Mechanics"),
    "두산테스나": ("131970.KQ", "Doosan Tesna"),
    "이수페타시스": ("007660.KS", "ISU Petasys"),
    "네오셈": ("253590.KQ", "NEOSEM"),
    "아비코전자": ("036010.KQ", "Abico Electronics"),
    "티엘비": ("356860.KQ", "TLB"),
    "심텍": ("222800.KQ", "Simmtech"),
    "원익ips": ("240810.KQ", "Wonik IPS"),
    "원익아이피에스": ("240810.KQ", "Wonik IPS"),
    "주성엔지니어링": ("036930.KQ", "Jusung Engineering"),
    "주성": ("036930.KQ", "Jusung Engineering"),
    "피에스케이": ("319660.KQ", "PSK"),
    "디아이": ("003160.KS", "DI"),
    "솔브레인": ("357780.KQ", "Solbraim"),
    "동진쎄미켐": ("005290.KS", "Dongjin Semichem"),
    "테크윙": ("089030.KQ", "Techwing"),
    "하나마이크론": ("067310.KQ", "Hana Micron"),
    "db하이텍": ("000990.KS", "DB HiTek"),
    # ── Battery / EV / 2차전지 ──
    "lg에너지솔루션": ("373220.KS", "LG Energy Solution"),
    "엘지에너지솔루션": ("373220.KS", "LG Energy Solution"),
    "lg에너지": ("373220.KS", "LG Energy Solution"),
    "삼성sdi": ("006400.KS", "Samsung SDI"),
    "에코프로비엠": ("247540.KQ", "EcoPro BM"),
    "에코프로": ("086520.KQ", "EcoPro"),
    "포스코퓨처엠": ("003670.KS", "POSCO Future M"),
    "엘앤에프": ("066970.KQ", "L&F"),
    "sk이노베이션": ("096770.KS", "SK Innovation"),
    "sk온": ("096770.KS", "SK Innovation"),
    "lg화학": ("051910.KS", "LG Chem"),
    "lg chem": ("051910.KS", "LG Chem"),
    "포스코홀딩스": ("005490.KS", "POSCO Holdings"),
    "posco": ("005490.KS", "POSCO Holdings"),
    # ── Automotive ──
    "현대자동차": ("005380.KS", "Hyundai Motor"),
    "현대차": ("005380.KS", "Hyundai Motor"),
    "현차": ("005380.KS", "Hyundai Motor"),
    "hyundai motor": ("005380.KS", "Hyundai Motor"),
    "기아": ("000270.KS", "Kia"),
    "kia": ("000270.KS", "Kia"),
    "현대모비스": ("012330.KS", "Hyundai Mobis"),
    "모비스": ("012330.KS", "Hyundai Mobis"),
    "만도": ("204320.KS", "Mando"),
    "hl만도": ("204320.KS", "Mando"),
    # ── Shipbuilding / Marine ──
    "hd현대중공업": ("329180.KS", "HD Hyundai Heavy Industries"),
    "현대중공업": ("329180.KS", "HD Hyundai Heavy Industries"),
    "한화오션": ("042660.KS", "Hanwha Ocean"),
    "한화오선": ("042660.KS", "Hanwha Ocean"),
    "삼성중공업": ("010140.KS", "Samsung Heavy Industries"),
    "hd한국조선해양": ("009540.KS", "HD Korea Shipbuilding"),
    "한국조선해양": ("009540.KS", "HD Korea Shipbuilding"),
    # ── Defense / Aerospace ──
    "한화에어로스페이스": ("012450.KS", "Hanwha Aerospace"),
    "한화에어로": ("012450.KS", "Hanwha Aerospace"),
    "한화방산": ("012450.KS", "Hanwha Aerospace"),
    "한국항공우주": ("047810.KS", "Korea Aerospace Industries"),
    "kai": ("047810.KS", "Korea Aerospace Industries"),
    "현대로템": ("064350.KS", "Hyundai Rotem"),
    "로템": ("064350.KS", "Hyundai Rotem"),
    "lig넥스원": ("079550.KS", "LIG Nex1"),
    "한화시스템": ("272210.KS", "Hanwha Systems"),
    # ── Power / Grid / Electrical Equipment ──
    "hd현대일렉트릭": ("267260.KS", "HD Hyundai Electric"),
    "현대일렉트릭": ("267260.KS", "HD Hyundai Electric"),
    "효성중공업": ("298040.KS", "Hyosung Heavy Industries"),
    "일진전기": ("103590.KS", "Iljin Electric"),
    "ls일렉트릭": ("010120.KS", "LS Electric"),
    "ls electric": ("010120.KS", "LS Electric"),
    "한국전력": ("015760.KS", "KEPCO"),
    "한전": ("015760.KS", "KEPCO"),
    "kepco": ("015760.KS", "KEPCO"),
    "두산에너빌리티": ("034020.KS", "Doosan Enerbility"),
    # ── Bio / Pharma ──
    "삼성바이오로직스": ("207940.KS", "Samsung Biologics"),
    "삼성바이오": ("207940.KS", "Samsung Biologics"),
    "삼바": ("207940.KS", "Samsung Biologics"),
    "셀트리온": ("068270.KS", "Celltrion"),
    "celltrion": ("068270.KS", "Celltrion"),
    "유한양행": ("000100.KS", "Yuhan"),
    "한미약품": ("128940.KS", "Hanmi Pharm"),
    "녹십자": ("006280.KS", "GC Biopharma"),
    "sk바이오팜": ("326030.KS", "SK Biopharmaceuticals"),
    "sk바이오사이언스": ("302440.KS", "SK Bioscience"),
    "알테오젠": ("196170.KQ", "Alteogen"),
    "리가켐바이오": ("141080.KQ", "LegoChem Biosciences"),
    "리가켐": ("141080.KQ", "LegoChem Biosciences"),
    "에이비엘바이오": ("298380.KQ", "ABL Bio"),
    # ── Platform / IT / Internet ──
    "네이버": ("035420.KS", "NAVER"),
    "naver": ("035420.KS", "NAVER"),
    "카카오": ("035720.KS", "Kakao"),
    "kakao": ("035720.KS", "Kakao"),
    "카카오뱅크": ("323410.KS", "KakaoBank"),
    "카카오페이": ("377300.KS", "KakaoPay"),
    "크래프톤": ("259960.KS", "Krafton"),
    "krafton": ("259960.KS", "Krafton"),
    "엔씨소프트": ("036570.KS", "NCSoft"),
    "ncsoft": ("036570.KS", "NCSoft"),
    "넷마블": ("251270.KS", "Netmarble"),
    "펄어비스": ("263750.KS", "Pearl Abyss"),
    "컴투스": ("078340.KS", "Com2uS"),
    "하이브": ("352820.KS", "HYBE"),
    "hybe": ("352820.KS", "HYBE"),
    "jyp엔터": ("035900.KQ", "JYP Entertainment"),
    "jyp": ("035900.KQ", "JYP Entertainment"),
    "sm엔터": ("041510.KS", "SM Entertainment"),
    "에스엠": ("041510.KS", "SM Entertainment"),
    "yg엔터": ("122870.KQ", "YG Entertainment"),
    # ── Construction / Real Estate ──
    "현대건설": ("000720.KS", "Hyundai E&C"),
    "대우건설": ("047040.KS", "Daewoo E&C"),
    "삼성물산": ("028260.KS", "Samsung C&T"),
    "gs건설": ("006360.KS", "GS E&C"),
    "dl이앤씨": ("375500.KS", "DL E&C"),
    # ── Finance / Securities / Insurance ──
    "kb금융": ("105560.KS", "KB Financial"),
    "신한금융": ("055550.KS", "Shinhan Financial"),
    "신한지주": ("055550.KS", "Shinhan Financial"),
    "하나금융": ("086790.KS", "Hana Financial"),
    "하나금융지주": ("086790.KS", "Hana Financial"),
    "우리금융": ("316140.KS", "Woori Financial"),
    "미래에셋증권": ("006800.KS", "Mirae Asset Securities"),
    "미래에셋": ("006800.KS", "Mirae Asset Securities"),
    "nh투자증권": ("005940.KS", "NH Investment & Securities"),
    "한국투자증권": ("071050.KS", "Korea Investment Holdings"),
    "삼성생명": ("032830.KS", "Samsung Life"),
    "삼성화재": ("000810.KS", "Samsung Fire & Marine"),
    "db손해보험": ("005830.KS", "DB Insurance"),
    # ── Chemicals / Refinery ──
    "롯데케미칼": ("011170.KS", "Lotte Chemical"),
    "한화솔루션": ("009830.KS", "Hanwha Solutions"),
    "금호석유화학": ("011780.KS", "Kumho Petrochemical"),
    "에쓰오일": ("010950.KS", "S-Oil"),
    "s-oil": ("010950.KS", "S-Oil"),
    # ── Telecom / Utility ──
    "sk텔레콤": ("017670.KS", "SK Telecom"),
    "skt": ("017670.KS", "SK Telecom"),
    "kt": ("030200.KS", "KT"),
    "lg유플러스": ("032640.KS", "LG Uplus"),
    # ── Retail / Consumer ──
    "cj제일제당": ("097950.KS", "CJ CheilJedang"),
    "kt&g": ("033780.KS", "KT&G"),
    "아모레퍼시픽": ("090430.KS", "Amorepacific"),
    "아모레": ("090430.KS", "Amorepacific"),
    "lg생활건강": ("051900.KS", "LG H&H"),
    "오리온": ("271560.KS", "Orion"),
    # ── Conglomerate / Holdings ──
    "sk": ("034730.KS", "SK Inc"),
    "sk주식회사": ("034730.KS", "SK Inc"),
    "lg": ("003550.KS", "LG Corp"),
    "한화": ("000880.KS", "Hanwha"),
    # ── Airlines / Travel ──
    "대한항공": ("003490.KS", "Korean Air"),
    "korean air": ("003490.KS", "Korean Air"),
    "제주항공": ("089590.KQ", "Jeju Air"),
    # ── US Tech (frequently mentioned in Korean finance channels) ──
    "tsmc": ("TSM", "TSMC"),
    "테슬라": ("TSLA", "Tesla"),
    "tesla": ("TSLA", "Tesla"),
    "애플": ("AAPL", "Apple"),
    "apple": ("AAPL", "Apple"),
    "마이크로소프트": ("MSFT", "Microsoft"),
    "microsoft": ("MSFT", "Microsoft"),
    "아마존": ("AMZN", "Amazon"),
    "amazon": ("AMZN", "Amazon"),
    "구글": ("GOOGL", "Alphabet"),
    "알파벳": ("GOOGL", "Alphabet"),
    "메타": ("META", "Meta Platforms"),
    "meta": ("META", "Meta Platforms"),
    "amd": ("AMD", "AMD"),
    "인텔": ("INTC", "Intel"),
    "intel": ("INTC", "Intel"),
    "arm": ("ARM", "ARM Holdings"),
    "팔란티어": ("PLTR", "Palantir"),
    "palantir": ("PLTR", "Palantir"),
    "asml": ("ASML", "ASML"),
}

SECTOR_STOCKS: dict[str, list[tuple[str, str]]] = {
    "growth_tech": [("035420.KS", "NAVER"), ("035720.KS", "Kakao")],
    "real_estate": [("330590.KS", "Lotte REIT"), ("365550.KS", "ESR Kendall Square REIT")],
    "construction": [("000720.KS", "Hyundai E&C"), ("047040.KS", "Daewoo E&C")],
    "securities": [("006800.KS", "Mirae Asset Securities"), ("005940.KS", "NH Investment & Securities")],
    "banks": [("105560.KS", "KB Financial"), ("055550.KS", "Shinhan Financial")],
    "insurance": [("032830.KS", "Samsung Life"), ("005830.KS", "DB Insurance")],
    "exporters": [("005380.KS", "Hyundai Motor"), ("000270.KS", "Kia")],
    "shipbuilding": [("329180.KS", "HD Hyundai Heavy Industries"), ("042660.KS", "Hanwha Ocean")],
    "refiners": [("010950.KS", "S-Oil"), ("096770.KS", "SK Innovation")],
    "airlines": [("003490.KS", "Korean Air"), ("089590.KQ", "Jeju Air")],
    "chemicals": [("051910.KS", "LG Chem"), ("011170.KS", "Lotte Chemical")],
    "cyclicals": [("005930.KS", "Samsung Electronics"), ("000660.KS", "SK hynix")],
    "defensives": [("033780.KS", "KT&G"), ("097950.KS", "CJ CheilJedang")],
    "telecom": [("017670.KS", "SK Telecom"), ("030200.KS", "KT")],
    "utilities": [("015760.KS", "KEPCO"), ("015590.KS", "KEPCO E&C")],
    "importers": [("003490.KS", "Korean Air"), ("051910.KS", "LG Chem")],
    "semiconductors": [("005930.KS", "Samsung Electronics"), ("000660.KS", "SK hynix")],
    "semicap": [("042700.KS", "Hanmi Semiconductor"), ("240810.KQ", "Wonik IPS")],
    "ai_platforms": [("035420.KS", "NAVER"), ("035720.KS", "Kakao")],
    "defense": [("012450.KS", "Hanwha Aerospace"), ("047810.KS", "Korea Aerospace Industries")],
    # ── New sectors ──
    "battery": [("373220.KS", "LG Energy Solution"), ("006400.KS", "Samsung SDI"), ("247540.KQ", "EcoPro BM")],
    "bio": [("207940.KS", "Samsung Biologics"), ("068270.KS", "Celltrion"), ("196170.KQ", "Alteogen")],
    "entertainment": [("352820.KS", "HYBE"), ("035900.KQ", "JYP Entertainment"), ("041510.KS", "SM Entertainment")],
    "gaming": [("259960.KS", "Krafton"), ("036570.KS", "NCSoft"), ("251270.KS", "Netmarble")],
    "power_equipment": [("267260.KS", "HD Hyundai Electric"), ("298040.KS", "Hyosung Heavy Industries"), ("010120.KS", "LS Electric")],
    "auto_parts": [("012330.KS", "Hyundai Mobis"), ("204320.KS", "Mando")],
    "cosmetics": [("090430.KS", "Amorepacific"), ("051900.KS", "LG H&H")],
}

COMPANY_PATTERN_STRINGS = [
    # Semiconductors
    r"엔비디아", r"nvidia", r"삼성전자", r"삼성", r"sk하이닉스", r"hynix", r"tsmc", r"브로드컴", r"broadcom",
    r"마이크론", r"micron", r"한미반도체", r"리노공업", r"원익", r"주성", r"이수페타시스",
    r"가온칩스", r"심텍", r"두산테스나", r"네오셈", r"솔브레인", r"동진쎄미켐", r"테크윙",
    r"db하이텍", r"하나마이크론",
    # Battery / EV
    r"lg에너지", r"삼성sdi", r"에코프로", r"포스코퓨처엠", r"엘앤에프",
    r"2차전지", r"배터리",
    # Auto
    r"현대차", r"현대자동차", r"기아", r"현대모비스", r"만도",
    # Shipbuilding
    r"현대중공업", r"hd현대중공업", r"한화오션", r"삼성중공업", r"한국조선해양",
    # Defense
    r"한화에어로", r"한국항공우주", r"현대로템", r"lig넥스원", r"한화시스템",
    # Power / Grid
    r"hd현대일렉트릭", r"효성중공업", r"일진전기", r"ls일렉트릭",
    r"hd hyundai electric", r"hyosung heavy", r"한국전력", r"한전", r"두산에너빌리티",
    # Bio
    r"삼성바이오", r"셀트리온", r"알테오젠", r"리가켐", r"유한양행", r"한미약품",
    # Platform / IT
    r"네이버", r"카카오", r"크래프톤", r"엔씨소프트", r"넷마블",
    # Entertainment
    r"하이브", r"hybe", r"jyp", r"sm엔터",
    # Finance
    r"kb금융", r"신한금융", r"미래에셋",
    # US frequently mentioned
    r"테슬라", r"tesla", r"애플", r"마이크로소프트", r"아마존", r"구글", r"메타", r"팔란티어",
    r"cpo", r"groq", r"grox", r"asml", r"arm",
]
COMPANY_PATTERNS = [re.compile(pattern, re.I) for pattern in COMPANY_PATTERN_STRINGS]


# ── Korean ticker resolver ────────────────────────────────────────────

_HANGUL_RE = re.compile(r"[가-힣]")
_NORMALIZE_NAME_RE = re.compile(r"[^0-9a-z가-힣]+")


def _contains_hangul(text: str) -> bool:
    return bool(_HANGUL_RE.search(text))


def _normalize_company_name(name: str) -> str:
    return _NORMALIZE_NAME_RE.sub("", name.strip().lower())

# Build reverse index: all Korean keys in COMPANY_MAP → (ticker, english_name)
_KR_NORMALIZED_INDEX: dict[str, tuple[str, str]] = {}
for _key, (_ticker, _eng) in COMPANY_MAP.items():
    # Only index Korean-language keys (contains Hangul)
    if _contains_hangul(_key):
        _KR_NORMALIZED_INDEX[_normalize_company_name(_key)] = (_ticker, _eng)

# Common suffixes to strip for fuzzy matching
_KR_SUFFIXES = (
    "전자", "화학", "건설", "증권", "보험", "카드", "지주", "그룹",
    "홀딩스", "솔루션", "솔루션스", "에너지", "머티리얼즈",
)


def resolve_kr_ticker(name: str) -> tuple[str, str] | None:
    """Resolve a Korean company name to (ticker, english_name).

    Tries normalized exact match first, then strips common suffixes for fuzzy matching.
    Returns None if no match found.
    """
    if not name:
        return None
    if not _contains_hangul(name):
        return None
    normalized = _normalize_company_name(name)
    if not normalized:
        return None

    result = _KR_NORMALIZED_INDEX.get(normalized)
    if result:
        return result

    # Try stripping common suffixes
    for suffix in _KR_SUFFIXES:
        suffix_normalized = _normalize_company_name(suffix)
        if normalized.endswith(suffix_normalized) and len(normalized) > len(suffix_normalized):
            stem = normalized[: -len(suffix_normalized)]
            result = _KR_NORMALIZED_INDEX.get(stem)
            if result:
                return result

    return None
