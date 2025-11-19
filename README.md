\# Proje: Change Data Capture (CDC) – SQL’den NoSQL’e Veri Aktarımı

Bu proje, bir PostgreSQL veritabanındaki değişiklikleri yakalayarak Python aracılığıyla bir MongoDB veritabanına aktaran basit bir prototiptir.



\## Kullanılan Teknolojiler

\- SQL Veritabanı: PostgreSQL

\- NoSQL Veritabanı: MongoDB

\- Programlama Dili: Python 3



\## Kurulum ve Çalıştırma Adımları

1\.  \*\*Ön Gereksinimler:\*\* Sisteminize Python, PostgreSQL ve MongoDB kurun.

2\.  \*\*Kütüphaneleri Yükleme:\*\* Terminalde proje klasöründeyken `pip install -r requirements.txt` komutunu çalıştırın.

3\.  \*\*Veritabanı Kurulumu:\*\* PostgreSQL'de `cdc\\\_project\\\_db` adında yeni bir veritabanı oluşturun. `setup.sql` dosyasındaki kodların tamamını bu veritabanında çalıştırın.

4\.  \*\*Uygulamayı Yapılandırma:\*\* `cdc\\\_app.py` dosyasını açın ve `db\\\_url` değişkenindeki `'sifre'` kısmını kendi PostgreSQL şifrenizle değiştirin.

5\.  \*\*Uygulamayı Çalıştırma:\*\* Terminalde `python cdc\\\_app.py` komutunu çalıştırın.

