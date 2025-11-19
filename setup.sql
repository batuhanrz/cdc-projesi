-- #############################################
-- #       PROJE KURULUM SQL SCRIPT'İ         #
-- #############################################

-- Adım 1: Tabloları Oluştur
-- ---------------------------------------------
CREATE TABLE Customers (
    customer_id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL
);

CREATE TABLE Orders (
    order_id SERIAL PRIMARY KEY,
    customer_id INT NOT NULL,
    product VARCHAR(255) NOT NULL,
    amount DECIMAL(10, 2) NOT NULL,
    status VARCHAR(50),
    CONSTRAINT fk_customer
        FOREIGN KEY (customer_id) 
        REFERENCES Customers(customer_id)
        ON DELETE CASCADE
);

CREATE TABLE Orders_log (
    log_id SERIAL PRIMARY KEY,
    order_id INT,
    operation_type VARCHAR(10) NOT NULL,
    old_data JSONB,
    new_data JSONB,
    changed_at TIMESTAMPTZ DEFAULT NOW(),
    is_processed BOOLEAN DEFAULT FALSE
);

-- Adım 2: Örnek Veri Ekle
-- ---------------------------------------------
INSERT INTO Customers (name, email) VALUES
('Ahmet Yılmaz', 'ahmet.yilmaz@example.com'),
('Ayşe Kaya', 'ayse.kaya@example.com');

INSERT INTO Orders (customer_id, product, amount, status) VALUES
(1, 'Laptop', 15000.75, 'Pending'),
(2, 'Klavye', 850.50, 'Shipped');


-- Adım 3: Trigger Fonksiyonunu ve Trigger'ı Oluştur
-- ---------------------------------------------
CREATE OR REPLACE FUNCTION log_orders_changes()
RETURNS TRIGGER AS $$
BEGIN
    IF (TG_OP = 'INSERT') THEN
        INSERT INTO Orders_log (order_id, operation_type, new_data)
        VALUES (NEW.order_id, 'INSERT', row_to_json(NEW)::jsonb);
    ELSIF (TG_OP = 'UPDATE') THEN
        INSERT INTO Orders_log (order_id, operation_type, old_data, new_data)
        VALUES (NEW.order_id, 'UPDATE', row_to_json(OLD)::jsonb, row_to_json(NEW)::jsonb);
    ELSIF (TG_OP = 'DELETE') THEN
        INSERT INTO Orders_log (order_id, operation_type, old_data)
        VALUES (OLD.order_id, 'DELETE', row_to_json(OLD)::jsonb);
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER orders_cdc_trigger
AFTER INSERT OR UPDATE OR DELETE ON Orders
FOR EACH ROW
EXECUTE FUNCTION log_orders_changes();

-- Kurulum tamamlandı mesajı
SELECT 'SQL Kurulumu basariyla tamamlandi.' as status;