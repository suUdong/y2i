from __future__ import annotations

import re

COMPANY_MAP = {
    "엔비디아": ("NVDA", "NVIDIA"),
    "nvidia": ("NVDA", "NVIDIA"),
    "삼성전자": ("005930.KS", "Samsung Electronics"),
    "삼성": ("005930.KS", "Samsung Electronics"),
    "sk하이닉스": ("000660.KS", "SK hynix"),
    "하이닉스": ("000660.KS", "SK hynix"),
    "마이크론": ("MU", "Micron"),
    "micron": ("MU", "Micron"),
    "브로드컴": ("AVGO", "Broadcom"),
    "broadcom": ("AVGO", "Broadcom"),
    "hd현대일렉트릭": ("267260.KS", "HD Hyundai Electric"),
    "효성중공업": ("298040.KS", "Hyosung Heavy Industries"),
    "일진전기": ("103590.KS", "Iljin Electric"),
    "리노공업": ("058470.KQ", "LEENO Industrial"),
    "marvell": ("MRVL", "Marvell"),
    "마벨": ("MRVL", "Marvell"),
    "가온칩스": ("399720.KQ", "Gaonchips"),
    "삼성전기": ("009150.KS", "Samsung Electro-Mechanics"),
    "두산테스나": ("131970.KQ", "Doosan Tesna"),
    "이수페타시스": ("007660.KS", "ISU Petasys"),
    "네오셈": ("253590.KQ", "NEOSEM"),
    "아비코전자": ("036010.KQ", "Abico Electronics"),
    "티엘비": ("356860.KQ", "TLB"),
    "심텍": ("222800.KQ", "Simmtech"),
}

SECTOR_STOCKS = {
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
}

COMPANY_PATTERN_STRINGS = [
    r"엔비디아", r"nvidia", r"삼성", r"sk하이닉스", r"hynix", r"tsmc", r"브로드컴", r"broadcom", r"마이크론", r"micron",
    r"한미반도체", r"리노공업", r"원익", r"주성", r"이수페타시스", r"cpo", r"groq", r"grox",
    r"hd현대일렉트릭", r"효성중공업", r"일진전기", r"hd hyundai electric", r"hyosung heavy",
]
COMPANY_PATTERNS = [re.compile(pattern, re.I) for pattern in COMPANY_PATTERN_STRINGS]
