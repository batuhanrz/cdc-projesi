import sqlalchemy
from pymongo import MongoClient
import time
import json

# --- AYARLAR: Lütfen bu kısmı kendi bilgilerinizle güncelleyin ---

# 1. PostgreSQL Bağlantı Bilgileri

db_url = "postgresql://postgres:1234@localhost:5432/cdc_project_db"

# 2. MongoDB Bağlantı Bilgileri
mongo_url = "mongodb://localhost:27017/"
mongo_db_name = "cdc_project_db"
mongo_collection_name = "order_change_logs"

# 3. Polling (Sorgulama) Aralığı (saniye cinsinden)
POLL_INTERVAL = 10

# --- UYGULAMA KODU ---

def connect_to_databases():
    """Veritabanlarına bağlantıları kurar ve bağlantı nesnelerini döndürür."""
    try:
        # PostgreSQL'e bağlan
        sql_engine = sqlalchemy.create_engine(db_url)
        sql_connection = sql_engine.connect()
        print("PostgreSQL'e başarıyla bağlanıldı.")
    except Exception as e:
        print(f"HATA: PostgreSQL'e bağlanılamadı. -> {e}")
        return None, None

    try:
        # MongoDB'ye bağlan
        mongo_client = MongoClient(mongo_url)
        # Sunucuya erişilip erişilemediğini test et
        mongo_client.server_info() 
        mongo_db = mongo_client[mongo_db_name]
        mongo_collection = mongo_db[mongo_collection_name]
        print("MongoDB'ye başarıyla bağlanıldı.")
    except Exception as e:
        print(f"HATA: MongoDB'ye bağlanılamadı. -> {e}")
        return None, None
        
    return sql_connection, mongo_collection

def poll_and_transfer_data():
    """
    Sonsuz bir döngüde SQL log tablosunu sorgular, yeni kayıtları MongoDB'ye aktarır
    ve aktarılan kayıtları işlenmiş olarak işaretler.
    """
    sql_conn, mongo_coll = connect_to_databases()
    
    # Bağlantı başarısızsa programı durdur
    if sql_conn is None or mongo_coll is None:
        print("Veritabanı bağlantı hatası nedeniyle program durduruluyor.")
        return

    print("\n--- CDC Uygulaması Başlatıldı. Yeni loglar dinleniyor... ---")
    while True:
        try:
            # 1. ADIM: İşlenmemiş logları SQL'den çek
            select_query = sqlalchemy.text("SELECT * FROM Orders_log WHERE is_processed = FALSE ORDER BY log_id ASC")
            new_logs = sql_conn.execute(select_query).fetchall()

            if not new_logs:
                print(f"Yeni log bulunamadı. {POLL_INTERVAL} saniye bekleniyor...")
            else:
                print(f"--- {len(new_logs)} adet yeni log bulundu! ---")
                log_ids_to_update = []
                for log in new_logs:
                    log_dict = dict(log._mapping) # SQLAlchemy sonucunu sözlüğe çevir
                    
                    # 2. ADIM: MongoDB'ye aktarılacak dökümanı hazırla
                    data_to_transfer = log_dict.get('new_data') or log_dict.get('old_data')
                    
                    if not data_to_transfer:
                        print(f"UYARI: Log ID {log_dict['log_id']} için veri bulunamadı, atlanıyor.")
                        continue
                        
                    mongo_doc = {
                        "operation": log_dict['operation_type'],
                        "table": "Orders",
                        "changed_at": log_dict['changed_at'].isoformat(),
                        **data_to_transfer
                    }
                    
                    # 3. ADIM: MongoDB'ye kaydet
                    mongo_coll.insert_one(mongo_doc)
                    print(f"Log ID {log_dict['log_id']} ({log_dict['operation_type']}) MongoDB'ye aktarıldı.")
                    log_ids_to_update.append(str(log_dict['log_id']))

                # 4. ADIM: İşlenen logları SQL'de 'is_processed = TRUE' olarak işaretle
                if log_ids_to_update:
                    update_query_str = f"UPDATE Orders_log SET is_processed = TRUE WHERE log_id IN ({','.join(log_ids_to_update)})"
                    update_query = sqlalchemy.text(update_query_str)
                    sql_conn.execute(update_query)
                    sql_conn.commit() # Değişiklikleri veritabanına onayla
                    print(f"İşlenen {len(log_ids_to_update)} log SQL'de işaretlendi.")

        except Exception as e:
            print(f"Döngü sırasında bir hata oluştu: {e}")
            time.sleep(POLL_INTERVAL)

        # Döngüler arasında bekle
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    poll_and_transfer_data()