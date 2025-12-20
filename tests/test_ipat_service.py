import pytest
from unittest.mock import MagicMock, patch
from app.services.ipat_service import _map_ticket_to_db_format, sync_and_save_past_history
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
    assert "1 件" in last_call_args["message"]

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
    assert "投票履歴は見つかりませんでした" in last_call_args["message"]

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
