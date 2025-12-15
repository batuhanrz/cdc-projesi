from pymongo import MongoClient

# --- AYARLAR ---
mongo_url = "mongodb://localhost:27017/"
mongo_db_name = "cdc_project_db"
mongo_collection_name = "order_change_logs"

def generate_report():
    try:
        # MongoDB Bağlantısı
        client = MongoClient(mongo_url)
        db = client[mongo_db_name]
        collection = db[mongo_collection_name]
        
        print("\n" + "="*50)
        print(f"      CDC PROJE ANALİZ RAPORU      ")
        print("="*50 + "\n")

        # ---------------------------------------------------------
        # 1. SON 10 DEĞİŞİKLİK
        # ---------------------------------------------------------
        print(f"SORGU 1: Son 10 Değişiklik Listeleniyor...")
        print("-" * 40)
        last_changes = collection.find({}, {"_id": 0}).sort("changed_at", -1).limit(10)
        
        count = 0
        for doc in last_changes:
            count += 1
            op_type = doc.get('operation', 'UNKNOWN').ljust(6)
            table = doc.get('table', 'UNKNOWN')
            tarih = str(doc.get('changed_at', '')).replace('T', ' ')[:19]
            print(f"{count}. [{tarih}] {op_type} -> Tablo: {table}")
        
        if count == 0: print("Log bulunamadı.")
        print("\n")

        # ---------------------------------------------------------
        # 2. EN ÇOK DEĞİŞİKLİK YAPILAN MÜŞTERİ
        # ---------------------------------------------------------
        print(f"SORGU 2: En Çok İşlem Gören Müşteri (Customer ID)")
        print("-" * 40)
        pipeline_customer = [
            {"$group": {"_id": "$customer_id", "total_ops": {"$sum": 1}}},
            {"$sort": {"total_ops": -1}},
            {"$limit": 1}
        ]
        result_customer = list(collection.aggregate(pipeline_customer))
        
        if result_customer:
            top_cust = result_customer[0]
            print(f"Müşteri ID   : {top_cust['_id']}")
            print(f"İşlem Sayısı : {top_cust['total_ops']}")
        else:
            print("Veri yok.")
        print("\n")

        # ---------------------------------------------------------
        # 3. HANGİ TABLO DAHA ÇOK GÜNCELLENMİŞ
        # ---------------------------------------------------------
        print(f"SORGU 3: En Çok Güncellenen Tablo")
        print("-" * 40)
        pipeline_table = [
            {"$group": {"_id": "$table", "total_ops": {"$sum": 1}}},
            {"$sort": {"total_ops": -1}},
            {"$limit": 1}
        ]
        result_table = list(collection.aggregate(pipeline_table))
        
        if result_table:
            top_tab = result_table[0]
            print(f"Tablo Adı    : {top_tab['_id']}")
            print(f"İşlem Sayısı : {top_tab['total_ops']}")
        else:
            print("Veri yok.")
        print("\n" + "="*50)

    except Exception as e:
        print(f"Hata: {e}")

if __name__ == "__main__":
    generate_report()