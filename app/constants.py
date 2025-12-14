# JRAの日本語 → 英語コード変換マップ (設計書準拠)
BET_TYPE_MAP = {
    "単勝": "WIN",
    "複勝": "PLACE",
    "枠連": "BRACKET_QUINELLA",
    "馬連": "QUINELLA",
    "ワイド": "QUINELLA_PLACE",
    "馬単": "EXACTA",
    "３連複": "TRIO",
    "３連単": "TRIFECTA"
}

# JRA競馬場名 → netkeiba互換コード
# https://github.com/umatoma/race-id-license/blob/main/LICENSE
RACE_COURSE_MAP = {
    "札幌": "01",
    "函館": "02",
    "福島": "03",
    "新潟": "04",
    "東京": "05",
    "中山": "06",
    "中京": "07",
    "京都": "08",
    "阪神": "09",
    "小倉": "10",
}