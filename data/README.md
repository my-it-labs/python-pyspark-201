# Dataset NovaShop

Tienda online de laboratorio. Todas las prácticas leen las mismas fuentes.

Regenerar (determinista, semilla 42):

```bash
python3 scripts/generate_novashop.py
```

Los recuentos oficiales están en `CANONICAL_COUNTS.json`.

## raw/

| Fichero | Formato | Filas | Suciedad deliberada |
|---------|---------|------:|---------------------|
| `customers.csv` | CSV | 250 | 5 países vacíos |
| `products.json` | JSON array | 60 | 3 `listPrice` vacíos; claves camelCase |
| `orders.csv` | CSV | 800 | Cabeceras camelCase; 12 sin cliente; 8 clientes huérfanos; 3 fechas `dd/mm/yyyy`; canales mezclados (`WEB`, `App`, `marketplace`) |
| `order_items.csv` | CSV | 2046 | 21 sin producto; 18 `P999`; 15 `qty=0`; 13 `discount>1`; precio como texto |
| `events.jsonl` | JSON Lines | 2500 | 80 sin `customer_id` |
| `profiles_v1.jsonl` | JSON Lines | 100 | CRM 2023 **plano** (`fullName`). Extra M08. |
| `profiles_v2.jsonl` | JSON Lines | 200 | CRM 2024 **anidado**. 50 ids solapan con v1; 8 sin `address.country`; 6 `orders_preview` vacío; 5 sin email work. |

## staging/ y curated/

Los escribes tú a partir de M02-03 y M07. No se versionan.
