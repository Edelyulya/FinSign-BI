BEGIN;

-- Очистим старые тестовые данные (если были)
TRUNCATE TABLE raw.ozon_stock;

-- Добавляем фиктивные остатки с ценами и регионами
INSERT INTO raw.ozon_stock (date, warehouse_name, region, product_id, sku, item_name, quantity, reserved, price)
VALUES
  (CURRENT_DATE - 6, 'Москва', 'RU-MOW', '1001', 'SKU-1001', 'Кофемашина Philips', 8, 2, 28990),
  (CURRENT_DATE - 5, 'Москва', 'RU-MOW', '1002', 'SKU-1002', 'Смартфон Samsung', 15, 3, 49990),
  (CURRENT_DATE - 4, 'Санкт-Петербург', 'RU-SPE', '1003', 'SKU-1003', 'Пылесос Dyson', 6, 1, 33990),
  (CURRENT_DATE - 3, 'Новосибирск', 'RU-NVS', '1004', 'SKU-1004', 'Телевизор LG', 10, 2, 45990),
  (CURRENT_DATE - 2, 'Екатеринбург', 'RU-SVE', '1005', 'SKU-1005', 'Наушники Sony', 25, 5, 7990),
  (CURRENT_DATE - 1, 'Москва', 'RU-MOW', '1006', 'SKU-1006', 'Планшет Huawei', 12, 4, 19990),
  (CURRENT_DATE,     'Казань', 'RU-TA',  '1007', 'SKU-1007', 'Умные часы Apple', 7, 1, 42990);

COMMIT;
