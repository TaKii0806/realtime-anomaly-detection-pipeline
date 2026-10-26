import database

def test_get_latest_id_empty_db(tmp_path, monkeypatch):
    """Bảng rỗng: get_latest_id() phải trả về 0, không được trả về None."""
    # 1. Trỏ DB_PATH vào file DB tạm trong tmp_path
    db_file = str(tmp_path / "empty.db")
    monkeypatch.setattr(database, "DB_PATH", db_file)
    
    # 2. Khởi tạo bảng rỗng
    database.init_db()
    
    # TODO: Gọi hàm get_latest_id() và dùng assert để kiểm tra kết quả == 0
    result = database.get_latest_id()
    assert result == 0


def test_get_latest_id_with_data(tmp_path, monkeypatch):
    """Bảng có 3 dòng: get_latest_id() phải trả về 3."""
    db_file = str(tmp_path / "data.db")
    monkeypatch.setattr(database, "DB_PATH", db_file)
    database.init_db()
    
    # Chuẩn bị 3 dòng mẫu
    records = [
        ("BTCUSDT", 60000.0, 0.1, "2026-10-25 10:00:00"),
        ("BTCUSDT", 60010.0, 0.2, "2026-10-25 10:00:01"),
        ("BTCUSDT", 60020.0, 0.3, "2026-10-25 10:00:02"),
    ]
    database.insert_transactions(records)
    assert database.get_latest_id() == 3
