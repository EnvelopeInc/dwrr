# Grocery product catalog subset builder

Build three **nested**, repeatable grocery catalog sizes using real identifiers and ingredient text from the USDA FoodData Central **Branded Foods CSV** release. About **80% are USDA food products**, and **20% are fictional nonfood general merchandise** (paper goods, laundry, dish/household cleaners, kitchen supplies, personal care, health/beauty, baby/family).

The USDA database contains food, **not** these general-merchandise products. Synthetic nonfood records have `source=SYNTHETIC_NONFOOD`, a `SYN-...` internal product ID, and **blank `gtin_upc`**. Do not interpret those as actual retail UPCs. USDA records retain their published UPC/GTIN and FDC IDs.

## Requirements

- Python 3.10+; **standard library only**
- USDA **Branded Foods CSV** ZIP from <https://fdc.nal.usda.gov/download-datasets/> (download the *Branded* CSV, not the JSON)
- A few GB of free disk space for the USDA archive, temporary SQLite food metadata index, and outputs

## Run

```sh
python3 build_catalogs.py \
  --usda ~/Downloads/FoodData_Central_branded_food_csv_2026-10-29.zip \
  --out ~/Development/synth-gen/data/catalog-subsets \
  --seed 17 \
  --sizes 500 6000 20000
```

Use the actual filename you downloaded. The builder can read the ZIP directly or the extracted folder containing `food.csv` and `branded_food.csv`.

If nutrition is useful for question generation, add `--nutrition`. This makes an **additional streaming pass** over the large `food_nutrient.csv` and writes selected energy, protein, fat, carbohydrate, fiber, and sodium facts into `nutrition.csv`. For full nutrition coverage, use the full USDA tables; this is only a lightweight selection.

You can also set `--as-of YYYY-MM-DD` and `--scratch /some/existing/directory`. The builder writes only to a new empty output directory and refuses to overwrite existing data.

### Output

```
catalog-subsets/
  catalog_00500/
    products.csv
    manifest.json
    nutrition.csv       # only with --nutrition
  catalog_06000/
    products.csv
    manifest.json
    nutrition.csv       # only with --nutrition
  catalog_20000/
    products.csv
    manifest.json
    nutrition.csv       # only with --nutrition
```

`products.csv` has these columns:

`product_id,source,department,store_category,gtin_upc,fdc_id,brand_owner,brand_name,description,ingredients,publication_date,serving_size,serving_size_unit,branded_food_category,supplier_category`

The three sizes are **nested within the same run**: the first 500 rows of the 6,000-product catalog are exactly the 500-product catalog, and the first 6,000 rows of the 20,000-product catalog are exactly the 6,000-product catalog. This makes it easier to vary corpus size without removing earlier products.

- Selection is quota-weighted across food and nonfood departments and seeded by stable GTIN hashes.
- When a GTIN has multiple available versions, the most recently published *selected candidate* is kept within its department.
- Nutrient data is always attached to specific FDC food IDs.
- `manifest.json` includes selection counts, source counts, seed, source ZIP hash, and the product CSV hash.
- Ingredients are copied from USDA as-is. No allergen or vegan classification is asserted in this builder; apply the simulator's heuristic classifier downstream and preserve `unknown` when the data is insufficient.
- If a department has too few qualifying USDA products, the builder uses qualifying products from other departments rather than inventing USDA catalog entries. It fails if it cannot reach the total size.

## Test

```sh
python3 -m unittest discover -s . -p 'test_*.py' -v
```

The offline tests create a small USDA-shaped ZIP, exercise GTIN validation, archive reading, repeatability, optional nutrition, nested selections, and synthetic-vs-real identifier separation. An additional 45,000-row synthetic fixture was used to verify all three production target sizes; **the full official USDA 2026 download has not been run in this environment**.

## Notes

- This is a **product-catalog subset**, not a simulated 250-store retail chain. Its output can be fed to `synth-gen` to construct distributor and retailer SKU mappings, stores, inventories, and question sets.
- The nonfood brands, names, and product IDs are fictional, not authoritative product assignments. If a real nonfood catalog becomes available, replace that portion without modifying USDA product identifiers.
- The latest version of a particular UPC can be category-sensitive: a reclassification between department buckets may not be fully normalized. For high-integrity historical catalog studies, build an explicit global GTIN-version table first.
- USDA dataset documentation: <https://fdc.nal.usda.gov/portal-data/external/dataDictionary>
