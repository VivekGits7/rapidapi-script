# How the Auto-Parts Dump Works (and How We Limit It)

A simple walkthrough of how we pull car-parts data from the RapidAPI catalog, and
**how we keep it from pulling too much.**

## The big idea — we don't grab everything

The catalog is huge. If we pulled every car, every part, in full depth, we'd burn
through our monthly API quota instantly. So we **save everything we see, but only
go "deep" on the important bits:**

- **We only dump the cars we choose.** A simple list (`dump_targets.csv`) says which
  makes + models to dump. Nothing outside that list is touched.
- **Per model → only the newest 5 cars get the full treatment.** A model can have
  dozens of engine versions. We keep a record of all of them, but only fully crawl
  the **latest 5**.
- **Per car + category → only the top 2 parts get full details.** A brake category
  might list 800 parts. We save all 800 names, but only pull the **full detail
  (specs, OEM numbers, fitment, etc.) for the top 2.**
- **We stop before the limit.** The plan allows 100,000 calls/month — we stop at
  **99,500** so we never overshoot, and pick up later where we left off.
- **Everything is fetched in English + Arabic** at the same time.

> Because we *save* everything and only *skip the deep part*, we can always go back
> later and grab more (raise the "5" or the "2") without re-doing the work.

The two limit numbers live in config:

| Setting | Value | Means |
|---|---|---|
| `MAX_VEHICLES_PER_MODEL` | **5** | Newest 5 cars per model get fully crawled |
| `MAX_ARTICLES_PER_CATEGORY` | **2** | Top 2 parts per (car, category) get full details |
| monthly cap | **99,500** | Calls/month before we pause |

---

## The stages (with what goes in and what comes out)

### 1. Reference data (languages, countries, car types)
Pulled once. No limit.

**Input**
```json
{ "endpoint": "/countries/list" }
```
**Output**
```json
{ "countries": [ { "id": 80, "couCode": "ET", "countryName": "Egypt" } ] }
```
*(In Arabic the same call returns `"countryName": "جمهورية مصر العربية"`.)*

---

### 2. Models for a make
We don't search for models — we already know them from our list (`dump_targets.csv`,
which already holds each make + model we want to dump). We just grab the Arabic
names once per make.

**Input**
```json
{ "typeId": 1, "manufacturerId": 2, "langId": 4, "countryFilterId": 80 }
```
**Output**
```json
{ "models": [ { "modelId": 4635, "modelName": "156 Sportwagon (932_)", "modelYearFrom": "1997-02-01", "modelYearTo": "2006-05-01" } ] }
```

---

### 3. Cars (engine versions) of a model
**Limit: we save them all, then keep only the newest 5 for the deep crawl.**

**Input**
```json
{ "typeId": 1, "modelId": 4635, "langId": 4, "countryFilterId": 80 }
```
**Output**
```json
{ "modelTypes": [
  { "vehicleId": 22625, "typeEngineName": "1.9 JTD", "fuelType": "Diesel",
    "bodyType": "Estate", "powerKw": "81", "constructionIntervalStart": "2002-09-01" }
] }
```

---

### 4. Categories for a car
Pulled for each of the 5 cars. The whole category tree, English + Arabic.

**Input**
```json
{ "typeId": 1, "vehicleId": 22625, "langId": 4 }
```
**Output**
```json
{ "categories": {
  "Braking System": { "categoryId": 100590, "categoryName": "Braking System", "children": { "...": {} } }
} }
```

---

### 5. Parts list for a (car + category)
**Limit: we save the whole list with a rank (position), but pull NO details here.**

**Input**
```json
{ "typeId": 1, "vehicleId": 22625, "categoryId": 100598, "langId": 4 }
```
**Output**
```json
{ "articles": [
  { "articleId": 6489966, "articleNo": "D20804", "supplierName": "1A FIRST AUTOMOTIVE",
    "articleProductName": "Fuel Filter", "s3image": "https://.../x.webp" }
] }
```
*(In Arabic: `"articleProductName": "فلتر الوقود"`.)*

---

### 6. Full part details
**Limit: only for the top 2 parts of each (car + category).** This one call returns
*everything* about a part — specs, OEM numbers, barcode, image, and the list of
cars it fits.

**Input**
```json
{ "typeId": 1, "articleId": 6489966, "langId": 4, "countryFilterId": 80 }
```
**Output**
```json
{ "article": {
  "articleProductName": "Fuel Filter",
  "allSpecifications": [ { "criteriaName": "Height [mm]", "criteriaValue": "150" } ],
  "oemNo": [ { "oemBrand": "BMW", "oemDisplayNo": "13 32 7 512 019" } ],
  "eanNo": { "eanNumbers": "4047024343207" },
  "compatibleCars": [ { "vehicleId": 22625, "manufacturerName": "ALFA ROMEO", "modelName": "156", "typeEngineName": "1.9 JTD" } ]
} }
```
*(In Arabic the names + spec labels come back translated, e.g. `"criteriaName": "الارتفاع [مم]"`.)*

---

## In one line per stage

| Stage | What we save | What we limit |
|---|---|---|
| Cars | every engine version | only newest **5** crawled further |
| Parts list | every part + its rank | (nothing — all saved) |
| Part details | — | only top **2** per category get full details |
| Whole run | — | pause at **99,500** calls/month |
