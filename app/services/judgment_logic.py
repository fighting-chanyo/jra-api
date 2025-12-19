import itertools
from typing import List, Dict, Any, Tuple, Set
from app.schemas import Ticket, PayoutData, PayoutItem

class JudgmentLogic:
    @staticmethod
    def judge_ticket(ticket: Ticket, result_1st: int, result_2nd: int, result_3rd: int, payout_data: PayoutData) -> Tuple[str, int]:
        """
        チケットの的中判定を行い、(status, payout) を返す
        status: HIT or LOSE
        payout: 払戻金合計
        """
        bet_type = ticket.bet_type
        content = ticket.content
        method = ticket.buy_type # NORMAL, BOX, NAGASHI, FORMATION (from parsers.py)
        
        # 払戻データがない場合は判定不能（あるいはハズレ扱いだが、通常はデータがある前提）
        if not payout_data:
            return "LOSE", 0

        # 式別に対応する払戻リストを取得
        payout_items: List[PayoutItem] = getattr(payout_data, bet_type, [])
        if not payout_items:
            return "LOSE", 0

        # ユーザーの買い目を展開して、整数のタプル/リストの集合にする
        # 各要素は [horse1, horse2, ...]
        # ただし、着順指定ありの流し（マルチなし）の場合は展開せずに判定する
        is_ordered_nagashi = (
            method == "NAGASHI" and 
            not content.get("multi", False) and 
            bet_type in ["TRIFECTA", "EXACTA", "TAN"] and
            bool(content.get("positions"))
        )

        user_combinations = []
        if not is_ordered_nagashi:
            user_combinations = JudgmentLogic._expand_combinations(bet_type, method, content)
        
        total_payout = 0
        hit_count = 0

        # 各的中組み合わせ（正解）について、ユーザーの買い目に含まれているかチェック
        for item in payout_items:
            winning_horses = item.horse # List[int]
            money = item.money
            
            is_hit = False
            if is_ordered_nagashi:
                is_hit = JudgmentLogic._is_hit_nagashi_ordered(content, winning_horses)
            else:
                is_hit = JudgmentLogic._is_hit(bet_type, winning_horses, user_combinations)

            # 正解がユーザーの買い目に含まれるか
            if is_hit:
                # 的中
                # 1点あたりの金額 * (配当 / 100)
                # amount_per_point は100円単位とは限らない（例: 100円）
                # money は100円あたりの配当
                payout_amount = ticket.amount_per_point * money // 100
                total_payout += payout_amount
                hit_count += 1

        if hit_count > 0:
            return "HIT", total_payout
        else:
            return "LOSE", 0

    @staticmethod
    def _is_hit_nagashi_ordered(content: Dict[str, Any], winning_horses: List[int]) -> bool:
        """
        着順指定あり・マルチなしの流し投票の的中判定
        Step A: 軸の判定
        Step B: 相手の判定
        """
        axis = [int(x) for x in content.get("axis", [])]
        partners = [int(x) for x in content.get("partners", [])]
        positions = content.get("positions", [])
        
        if not positions or len(axis) != len(positions):
            return False

        # Step A: 軸の判定
        for horse_num, pos in zip(axis, positions):
            # pos は 1-based index
            idx = pos - 1
            if idx < 0 or idx >= len(winning_horses):
                return False
            
            if winning_horses[idx] != horse_num:
                return False

        # Step B: 相手の判定
        axis_indices = {p - 1 for p in positions}
        remaining_indices = [i for i in range(len(winning_horses)) if i not in axis_indices]
        remaining_results = [winning_horses[i] for i in remaining_indices]
        
        for res_horse in remaining_results:
            if res_horse not in partners:
                return False

        return True

    @staticmethod
    def _expand_combinations(bet_type: str, method: str, content: Dict[str, Any]) -> List[List[int]]:
        """
        買い目を具体的な組み合わせのリストに展開する
        戻り値: List[List[int]] (例: [[1], [2]] や [[1, 2], [1, 3]])
        """
        selections = content.get("selections", [])
        # selections は parsers.py により文字列のリストのリストになっている可能性がある
        # 数値に変換しておく
        
        def to_ints(str_list):
            return [int(x) for x in str_list]

        if method == "BOX":
            # selections[0] に馬番リストが入っている
            horses = to_ints(selections[0])
            r = JudgmentLogic._get_combination_r(bet_type)
            # 順列か組み合わせか
            if bet_type in ["EXACTA", "TRIFECTA"]:
                return [list(x) for x in itertools.permutations(horses, r)]
            else:
                return [list(x) for x in itertools.combinations(horses, r)]

        elif method == "NAGASHI":
            axis = to_ints(content.get("axis", []))
            partners = to_ints(content.get("partners", []))
            multi = content.get("multi", False)
            r = JudgmentLogic._get_combination_r(bet_type)
            
            combs = []
            # 相手の必要数 = 全体数 - 軸数
            needed_partners = r - len(axis)
            
            if needed_partners < 0: return [] # エラー

            # 相手から必要数を選ぶ組み合わせ
            partner_combs = itertools.combinations(partners, needed_partners)
            
            for p_comb in partner_combs:
                # 軸 + 選んだ相手
                base_set = axis + list(p_comb)
                
                if multi:
                    # マルチの場合: base_set の順列/組み合わせ（式別による）
                    if bet_type in ["EXACTA", "TRIFECTA"]:
                        combs.extend([list(x) for x in itertools.permutations(base_set, r)])
                    else:
                        # 順序関係ない式別ならそのまま（ただしBOXと同じになるのでマルチの意味は薄いが）
                        combs.append(base_set)
                else:
                    # マルチでない場合（通常流し）
                    if bet_type == "EXACTA":
                        # 軸1頭、相手1頭
                        for p in p_comb:
                            combs.append(axis + [p])
                    elif bet_type == "TRIFECTA":
                        # 軸1頭なら 軸 -> p1 -> p2 (pの順列)
                        # 軸2頭なら 軸1 -> 軸2 -> p1
                        # 相手同士の順列を考慮
                        for p_perm in itertools.permutations(p_comb):
                            combs.append(axis + list(p_perm))
                    else:
                        # 順序関係ない
                        combs.append(base_set)
            return combs

        elif method == "FORMATION":
            # selections は [ [1着候補], [2着候補], [3着候補] ] のようなリスト
            # 各候補から1つずつ選ぶ直積
            candidates = [to_ints(s) for s in selections]
            return [list(x) for x in itertools.product(*candidates)]

        else: # NORMAL
            # selections は [[1, 2], [3, 4]] のように、それぞれの買い目がリストになっている
            # parsers.py では selections = [re.findall(r'\d+', kumiban_str)] となっているので
            # 1つの買い目につき1つのリスト
            # 例: 馬連 1-2 -> [[1, 2]]
            # 例: 3連単 1-2-3 -> [[1, 2, 3]]
            # 複数行ある場合は呼び出し元でループしているはずだが、
            # content["selections"] が複数の買い目を含んでいる可能性もある？
            # parsers.py を見ると selections はリストのリスト。
            return [to_ints(s) for s in selections]

    @staticmethod
    def _get_combination_r(bet_type: str) -> int:
        if bet_type in ["WIN", "PLACE"]: return 1
        if bet_type in ["BRACKET_QUINELLA", "QUINELLA", "QUINELLA_PLACE", "EXACTA"]: return 2
        if bet_type in ["TRIO", "TRIFECTA"]: return 3
        return 1

    @staticmethod
    def _is_hit(bet_type: str, winning_horses: List[int], user_combinations: List[List[int]]) -> bool:
        """
        正解の馬番リストが、ユーザーの買い目リストのいずれかと一致するか
        """
        # 順序を気にする式別
        is_ordered = bet_type in ["EXACTA", "TRIFECTA"]
        
        # 単勝・複勝は1頭
        if bet_type in ["WIN", "PLACE"]:
            # winning_horses は [1] のようにリスト
            # user_combinations は [[1], [5]] など
            # 複勝の場合、winning_horses は [1] (1着), [2] (2着)... と別々に渡される前提
            # judge_ticket のループで payout_items を回しているので、
            # ここに来る winning_horses は「1つの的中組み合わせ」である。
            # 複勝の的中組み合わせは「3番」のように1頭。
            target = winning_horses
            for comb in user_combinations:
                # 単勝・複勝は馬番が一致すればOK
                # comb は [1] のようなリスト
                if target == comb:
                    return True
            return False

        # 枠連は馬番ではなく枠番で判定する必要があるが、
        # 今回のスコープでは馬番データしか持っていないため、枠連は正確に判定できない可能性がある。
        # ただし、Netkeibaの払戻データが「枠番」で返ってくるなら、
        # Ticketのcontentも「枠番」であれば一致判定できる。
        # parsers.py は馬/組番をそのままパースしている。
        # 枠連の場合、IPAT CSVには枠番が書かれているはず。
        # Netkeibaの払戻も枠番。
        # したがって、数値として一致すればOK。
        
        for comb in user_combinations:
            if is_ordered:
                # 順序完全一致
                if comb == winning_horses:
                    return True
            else:
                # 集合として一致
                if set(comb) == set(winning_horses):
                    return True
        
        return False
