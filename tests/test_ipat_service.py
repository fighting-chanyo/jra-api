import pytest
from unittest.mock import MagicMock, patch
from app.services.ipat_service import _map_ticket_to_db_format, sync_and_save_past_history, sync_and_save_recent_history
from app.schemas import IpatAuth

# Sample data for testing
SAMPLE_TICKET = {
    "raw": {
        "race_date_str": "20231224",
        "race_place": "中山",
        "race_number_str": "11",
        "receipt_no": "12345678",
        "line_no": 1
    },
    "parsed": {
        "bet_type": "WIN",
        "buy_type": "NORMAL",
        "content": {"type": "WIN", "selections": [["1"]]},
        "amount_per_point": 100,
        "total_cost": 1000,
        "payout": 0,
        "status": "PENDING"
    }
}

SAMPLE_AUTH = IpatAuth(
    inet_id="inetid",
    subscriber_number="12345678",
    password="password",
    pars_number="1234"
)

def test_map_ticket_to_db_format():
    user_id = "test_user"
    result = _map_ticket_to_db_format(SAMPLE_TICKET, user_id)

    assert result["user_id"] == user_id
    # 20231224 + 中山(06) + 11R -> 202312240611
    assert result["race_id"] == "202312240611"
    assert result["bet_type"] == "WIN"
    assert result["total_points"] == 10  # 1000 / 100
    assert result["receipt_unique_id"] is not None
    assert result["source"] == "IPAT_SYNC"


def test_receipt_unique_id_line_no_normalized():
    user_id = "test_user"
    t1 = {
        **SAMPLE_TICKET,
        "raw": {
            **SAMPLE_TICKET["raw"],
            "line_no": "01",
        },
    }
    t2 = {
        **SAMPLE_TICKET,
        "raw": {
            **SAMPLE_TICKET["raw"],
            "line_no": 1,
        },
    }
    r1 = _map_ticket_to_db_format(t1, user_id)
    r2 = _map_ticket_to_db_format(t2, user_id)
    assert r1["receipt_unique_id"] == r2["receipt_unique_id"]

@patch("app.services.ipat_service.get_supabase_client")
@patch("app.services.ipat_service.scrape_past_history_csv")
def test_sync_and_save_past_history_success(mock_scrape, mock_get_client):
    # Setup mocks
    mock_supabase = MagicMock()
    mock_get_client.return_value = mock_supabase
    
    # Mock scraping result
    mock_scrape.return_value = [SAMPLE_TICKET]
    
    # Mock DB responses
    mock_supabase.table.return_value.update.return_value.eq.return_value.execute.return_value = {"data": [{"id": "log123"}], "error": None}
    mock_supabase.table.return_value.upsert.return_value.execute.return_value = {"data": [], "error": None}
    # Existing receipt_unique_id lookup (no existing => new_count=1)
    mock_supabase.table.return_value.select.return_value.in_.return_value.execute.return_value = {"data": [], "error": None}

    # Execute
    sync_and_save_past_history("log123", "user1", SAMPLE_AUTH)

    # Verify
    mock_scrape.assert_called_once_with(SAMPLE_AUTH)
    
    # Verify upsert called
    upsert_call = mock_supabase.table("tickets").upsert
    assert upsert_call.called
    args, kwargs = upsert_call.call_args
    assert len(args[0]) == 1
    assert args[0][0]["race_id"] == "202312240611"

    # Verify log update (COMPLETED)
    update_call = mock_supabase.table("sync_logs").update
    assert update_call.called
    # Check if the last call was for completion
    last_call_args = update_call.call_args[0][0]
    assert last_call_args["status"] == "COMPLETED"
    assert "1件の新しいデータが見つかりました" in last_call_args["message"]

@patch("app.services.ipat_service.get_supabase_client")
@patch("app.services.ipat_service.scrape_past_history_csv")
def test_sync_and_save_past_history_no_tickets(mock_scrape, mock_get_client):
    # Setup mocks
    mock_supabase = MagicMock()
    mock_get_client.return_value = mock_supabase
    
    # Mock scraping result (empty)
    mock_scrape.return_value = []

    # Execute
    sync_and_save_past_history("log123", "user1", SAMPLE_AUTH)

    # Verify
    mock_scrape.assert_called_once()
    
    # Verify upsert NOT called
    upsert_call = mock_supabase.table("tickets").upsert
    assert not upsert_call.called

    # Verify log update (COMPLETED with no tickets message)
    update_call = mock_supabase.table("sync_logs").update
    assert update_call.called
    last_call_args = update_call.call_args[0][0]
    assert last_call_args["status"] == "COMPLETED"
    assert "新しいデータは見つかりませんでした" in last_call_args["message"]

@patch("app.services.ipat_service.get_supabase_client")
@patch("app.services.ipat_service.scrape_past_history_csv")
def test_sync_and_save_past_history_error(mock_scrape, mock_get_client):
    # Setup mocks
    mock_supabase = MagicMock()
    mock_get_client.return_value = mock_supabase
    
    # Mock scraping error
    mock_scrape.side_effect = Exception("Login Failed: Invalid Credentials")

    # Execute
    sync_and_save_past_history("log123", "user1", SAMPLE_AUTH)

    # Verify log update (ERROR)
    update_call = mock_supabase.table("sync_logs").update
    assert update_call.called
    last_call_args = update_call.call_args[0][0]
    assert last_call_args["status"] == "ERROR"
    assert "ログインに失敗しました" in last_call_args["message"]


@patch("app.services.ipat_service.get_supabase_client")
@patch("app.services.ipat_service.scrape_recent_history")
@patch("app.services.race_service.RaceService")
def test_sync_and_save_recent_history_skip_existing(mock_race_service_cls, mock_scrape, mock_get_client):
    mock_supabase = MagicMock()

    # table(name) が同じ return_value を返すと、別テーブルの upsert が tickets の upsert と区別できない。
    # そのためテーブル別にモックを分ける。
    tickets_tbl = MagicMock()
    sync_logs_tbl = MagicMock()
    races_tbl = MagicMock()
    section_receipts_tbl = MagicMock()

    def _table(name: str):
        return {
            "tickets": tickets_tbl,
            "sync_logs": sync_logs_tbl,
            "races": races_tbl,
            "ipat_section_receipts": section_receipts_tbl,
        }[name]

    mock_supabase.table.side_effect = _table
    mock_get_client.return_value = mock_supabase

    mock_scrape.return_value = [SAMPLE_TICKET]

    existing_receipt_id = _map_ticket_to_db_format(SAMPLE_TICKET, "user1")["receipt_unique_id"]

    # --- races: current section anchor ---
    races_tbl.select.return_value.gte.return_value.lte.return_value.order.return_value.limit.return_value.execute.return_value = {
        "data": [{"date": "2023-12-24"}],
        "error": None,
    }
    # previous day existence check (stop immediately)
    races_tbl.select.return_value.eq.return_value.limit.return_value.execute.return_value = {
        "data": [],
        "error": None,
    }
    # section receipts: nothing existing
    section_receipts_tbl.select.return_value.eq.return_value.eq.return_value.execute.return_value = {
        "data": [],
        "error": None,
    }

    # receipt_unique_id lookup: already exists
    tickets_tbl.select.return_value.in_.return_value.execute.return_value = {
        "data": [{"receipt_unique_id": existing_receipt_id}],
        "error": None,
    }
    # sync_logs update
    sync_logs_tbl.update.return_value.eq.return_value.execute.return_value = {
        "data": [{"id": "log123"}],
        "error": None,
    }

    mock_race_service_cls.return_value = MagicMock()

    sync_and_save_recent_history("log123", "user1", SAMPLE_AUTH)

    # since existing, upsert should not be called for insert_records
    upsert_call = tickets_tbl.upsert
    assert not upsert_call.called
