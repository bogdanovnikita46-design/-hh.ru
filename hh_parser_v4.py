"""
Парсер вакансий hh.ru - версия 4
Исследование: требования к образованию в креативных professiyah Rossii

Ustanovka zavisimostej:
    pip install requests pandas beautifulsoup4

Zapusk (test - 1 klyuchevoe slovo, 1 stranica):
    python hh_parser_v4.py --test

Zapusk (polnyj):
    python hh_parser_v4.py

Osobennosti:
- Kazhdye SAVE_EVERY vakansij sohranyaetsya otdelnyj fajl PART_XXXX.csv
- Pri povtornom zapuske chitaet vse PART_*.csv i propuskaet uzhe sohranennye id
- V konce sobiraet vse chasti v odin FULL_*.csv
"""

import requests
import pandas as pd
import datetime
import time
import random
import argparse
import os
import glob
from bs4 import BeautifulSoup


# =====================================================
# NASTROJKI
# =====================================================

CONFIG_CSV  = "vacancies_config.csv"
OUTPUT_DIR  = "output"          # Papka dlya sohraneniya fajlov

AREA        = 113               # 113 = vsya Rossiya; 1 = Moskva
PERIOD      = None              # None = bez ogranichenij; chislo = poslednie N dnej
PER_PAGE    = 100               # Vakansij na stranicu (maks. 100)
PAGES_FULL  = 5                 # Stranic na zapros v polnom rezhime
PAGES_TEST  = 1                 # Stranic na zapros v test-rezhime
FIELD       = "name"            # "name" - v nazvanii; "all" - vo vseh polyah

USE_PROXIES = False

PAUSE_MIN   = 0.3
PAUSE_MAX   = 0.7

SAVE_EVERY  = 200               # Sohranenie kazhdye N novyh vakansij


# =====================================================
# ZAGRUZKA KONFIGA
# =====================================================

def load_search_config(csv_path):
    df = pd.read_csv(csv_path)
    config = []
    for _, row in df.iterrows():
        block = row["Block"]
        keywords_raw = str(row["Ключевые слова поиска (hh.ru)"])
        keywords = [
            k.strip().strip("«»\"'")
            for k in keywords_raw.split(",")
            if k.strip()
        ]
        for kw in keywords:
            config.append({"block": block, "keyword": kw})

    print(f"  Zagruzheno blokov: {df['Block'].nunique()}")
    print(f"  Vsego klyuchevyh slov: {len(config)}")
    return config


# =====================================================
# ZAGRUZKA UZhE SOBRANNYH ID
# =====================================================

def load_seen_ids(output_dir):
    """
    Chitaet vse PART_*.csv iz papki output i vozvraschaet
    mnozhestvo uzhe sobrannyh id vakansij.
    """
    seen_ids = set()
    files = glob.glob(os.path.join(output_dir, "PART_*.csv"))

    if not files:
        print("  Sohranennye fajly ne najdeny - nachinaem s nulya.")
        return seen_ids

    print(f"  Najdeno sohranennyh fajlov: {len(files)}")
    for fname in sorted(files):
        try:
            df = pd.read_csv(fname, usecols=["id"], dtype={"id": str})
            before = len(seen_ids)
            seen_ids.update(df["id"].tolist())
            print(f"    {os.path.basename(fname)}: {len(df)} strok, novyh id: {len(seen_ids) - before}")
        except Exception as e:
            print(f"    Oshibka chteniya {fname}: {e}")

    print(f"  Vsego uzhe sobrano vakansij: {len(seen_ids)}")
    return seen_ids


# =====================================================
# PROKSI
# =====================================================

_proxy_cache = None

def get_proxies():
    global _proxy_cache
    if _proxy_cache is None:
        _proxy_cache = _fetch_proxies() if USE_PROXIES else []
    return _proxy_cache

def _fetch_proxies():
    try:
        response = requests.get("https://free-proxy-list.net/", timeout=10)
        soup = BeautifulSoup(response.text, "html.parser")
        table = soup.find("table", {"class": "table table-striped table-bordered"})
        proxies = []
        for row in table.find_all("tr")[1:]:
            cols = row.find_all("td")
            if cols:
                proxies.append(f"http://{cols[0].text.strip()}:{cols[1].text.strip()}")
        print(f"  Zagruzheno proksi: {len(proxies)}")
        return proxies
    except Exception as e:
        print(f"  Proksi nedostupny: {e}. Rabotaem napryamuyu.")
        return []


# =====================================================
# HTTP-ZAPROSY
# =====================================================

def retry_request(url, params=None, retries=5, delay=5):
    headers = {"User-Agent": "CreativeJobsResearch/4.0 (educational project)"}
    proxies_list = get_proxies()

    for attempt in range(retries):
        proxy = {"http": random.choice(proxies_list)} if (USE_PROXIES and proxies_list) else None
        try:
            response = requests.get(url, params=params, headers=headers, proxies=proxy, timeout=15)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            print(f"    Popytka {attempt + 1}/{retries}: {e}")
            if attempt < retries - 1:
                time.sleep(delay)
            else:
                raise


# =====================================================
# ANALIZ TEKSTA
# =====================================================

def check_description(description):
    if not description:
        return {
            "upomyanuto_vysshee_v_tekste": False,
            "obrazovanie_ne_trebuetsya_v_tekste": False,
            "upomyanuto_portfolio": False,
            "upomyanut_opyt_v_tekste": False,
        }
    text = description.lower()
    return {
        "upomyanuto_vysshee_v_tekste": any(p in text for p in [
            "vysshee obrazovanie", "nalichie diploma", "profilnoe obrazovanie",
            "okonchennoe vysshee", "bakalavr", "magistr", "vuz", "universitet",
        ]),
        "obrazovanie_ne_trebuetsya_v_tekste": any(p in text for p in [
            "bez vysshego obrazovaniya", "obrazovanie ne vazhno",
            "obrazovanie ne trebuetsya", "diplom ne nuzhen",
            "portfolio vazhneye diploma",
        ]),
        "upomyanuto_portfolio": "portfolio" in text,
        "upomyanut_opyt_v_tekste": any(p in text for p in [
            "opyt raboty", "opyt ot", "let opyta", "goda opyta", "let praktiki",
        ]),
    }


# =====================================================
# SBOR VAKANSIJ
# =====================================================

def fetch_vacancy_detail(vacancy_id):
    response = retry_request(f"https://api.hh.ru/vacancies/{vacancy_id}")
    return response.json()


def build_dataframe(items):
    """Prevraschaet spisok vakansij v ploskij DataFrame."""
    if not items:
        return pd.DataFrame()

    df = pd.DataFrame(items)
    result = df.copy()

    # Razvorachivaem vlozhennye slovari cherez json_normalize
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, dict)).any():
            safe = df[col].apply(lambda x: x if isinstance(x, dict) else {})
            normalized = pd.json_normalize(safe.tolist())
            normalized.columns = [f"{col}_{c}" for c in normalized.columns]
            result = pd.concat([result.drop(columns=[col]), normalized], axis=1)

    # professional_roles - spisok, beryom pervyj element
    if "professional_roles" in df.columns:
        result["prof_role_id"]   = df["professional_roles"].apply(
            lambda x: x[0]["id"]   if isinstance(x, list) and x else None)
        result["prof_role_name"] = df["professional_roles"].apply(
            lambda x: x[0]["name"] if isinstance(x, list) and x else None)

    # Udalyaem ostavshiesya kolonki s dict/list
    while True:
        bad = [c for c in result.columns
               if result[c].apply(lambda x: isinstance(x, (dict, list))).any()]
        if not bad:
            break
        result.drop(columns=bad, inplace=True)

    # Prioritetnye kolonki - pervymi
    priority = [
        "id", "name", "block", "search_query",
        "employer_name", "area_name",
        "experience_name", "employment_name",
        "salary_from", "salary_to", "salary_currency",
        "education_id", "education_name",
        "upomyanuto_vysshee_v_tekste",
        "obrazovanie_ne_trebuetsya_v_tekste",
        "upomyanuto_portfolio",
        "upomyanut_opyt_v_tekste",
        "key_skills_flat",
        "prof_role_name",
        "published_at",
        "alternate_url",
    ]
    existing = [c for c in priority if c in result.columns]
    rest     = [c for c in result.columns if c not in priority]
    return result[existing + rest]


def save_part(items, output_dir, part_num, run_id):
    """Sohranit tekushchuyu partiyu vakansij v otdelnyj fajl."""
    df = build_dataframe(items)
    if df.empty:
        return
    filename = os.path.join(output_dir, f"PART_{part_num:04d}_{run_id}.csv")
    df.to_csv(filename, index=False, encoding="utf-8-sig")
    print(f"\n  SOHRANENO: {filename} ({len(df)} vakansij)\n")


def collect_vacancies(config, pages_to_parse, seen_ids, output_dir, run_id):
    """
    Sobiraet vakansii po vsem klyuchevym slovam.
    - Propuskaet id kotorye uzhe est v seen_ids
    - Kazhdye SAVE_EVERY novyh vakansij sohranyaet promezhutochnyj fajl
    """
    buffer   = []   # Tekushchij bufer - eshyo ne sohranennye vakansii
    part_num = len(glob.glob(os.path.join(output_dir, "PART_*.csv"))) + 1
    total_new = 0

    for entry in config:
        block   = entry["block"]
        keyword = entry["keyword"]
        print(f"\nZapros: [{block}] '{keyword}'")

        for page in range(pages_to_parse):
            params = {
                "page":     page,
                "per_page": PER_PAGE,
                "text":     f"!{keyword}",
                "area":     AREA,
                "field":    FIELD,
            }
            if PERIOD:
                params["period"] = PERIOD

            try:
                data = retry_request("https://api.hh.ru/vacancies", params=params).json()
            except Exception as e:
                print(f"  Oshibka poiska: {e}")
                continue

            items = data.get("items", [])
            if not items:
                break

            new_on_page = 0
            for item in items:
                vid = str(item.get("id"))

                # Propuskaem uzhe sohranennye
                if vid in seen_ids:
                    continue
                seen_ids.add(vid)

                try:
                    detail = fetch_vacancy_detail(vid)
                except Exception as e:
                    print(f"    Detali {vid}: {e}")
                    detail = item

                detail["block"]          = block
                detail["search_query"]   = keyword
                skills = detail.get("key_skills", [])
                detail["key_skills_flat"] = ", ".join(
                    s["name"] for s in skills if "name" in s)
                detail.update(check_description(
                    detail.get("description", "") or ""))

                buffer.append(detail)
                new_on_page += 1
                total_new   += 1

                # Promezhutochnoe sohranenie
                if len(buffer) >= SAVE_EVERY:
                    save_part(buffer, output_dir, part_num, run_id)
                    part_num += 1
                    buffer = []  # Ochishchaem bufer posle sohraneniya

                time.sleep(random.uniform(PAUSE_MIN, PAUSE_MAX))

            print(f"  Str. {page+1} | +{new_on_page} novyh | vsego za zapusk: {total_new}")

            if page >= data.get("pages", 1) - 1:
                break

    # Sohranenie ostatka buffera
    if buffer:
        save_part(buffer, output_dir, part_num, run_id)

    return total_new


def merge_parts(output_dir, run_id):
    """Sobrat vse PART_*.csv v odin FULL_*.csv."""
    files = sorted(glob.glob(os.path.join(output_dir, "PART_*.csv")))
    if not files:
        print("Net fajlov dlya ob\"edineniya.")
        return

    print(f"\nOb\"edinyayu {len(files)} fajlov...")
    dfs = []
    for f in files:
        try:
            dfs.append(pd.read_csv(f, dtype={"id": str}))
        except Exception as e:
            print(f"  Oshibka chteniya {f}: {e}")

    if not dfs:
        return

    full_df = pd.concat(dfs, ignore_index=True)
    # Udalyaem dublikaty na sluchaj peresechenij mezhdu chostyami
    full_df.drop_duplicates(subset=["id"], inplace=True)

    full_name = os.path.join(output_dir, f"FULL_{run_id}.csv")
    full_df.to_csv(full_name, index=False, encoding="utf-8-sig")
    print(f"ITOG: {full_name} ({len(full_df)} vakansij)")
    return full_name


# =====================================================
# ZAPUSK
# =====================================================

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--test", action="store_true",
                        help="Test: 1 klyuchevoe slovo, 1 stranica")
    args = parser.parse_args()

    run_id = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M")

    print("=" * 60)
    print("  Pars hh.ru v4 | Kreativnye professii | Obrazovanie")
    print("=" * 60)

    # Sozdaem papku dlya rezultatov esli net
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    print(f"  Papka dlya fajlov: {OUTPUT_DIR}/")

    # Proverka konfiga
    if not os.path.exists(CONFIG_CSV):
        print(f"\nFajl '{CONFIG_CSV}' ne najden!")
        print("  Polozhi vacancies_config.csv vту zhe papku chto i skript.")
        return

    # Zagruzka konfiga i uzhe sobrannyh id
    config   = load_search_config(CONFIG_CSV)
    seen_ids = load_seen_ids(OUTPUT_DIR)

    # Rezhim zapuska
    if args.test:
        config = config[:1]
        pages  = PAGES_TEST
        print(f"\nTEST: 1 klyuchevoe slovo, {pages} stranica (~{PER_PAGE} vakansij)")
    else:
        pages = PAGES_FULL
        print(f"\nPOLNYJ: {len(config)} slov x {pages} str.")
        if seen_ids:
            print(f"  Prodolzhaem s mesta ostanovki (uzhe est {len(seen_ids)} vakansij)")

    # Sbor
    total = collect_vacancies(config, pages, seen_ids, OUTPUT_DIR, run_id)
    print(f"\nSobrano novyh vakansij za etot zapusk: {total}")

    # Ob\"edinenie vseh chastej v odin fajl
    print("\nSobirayem vse chasti v odin fajl...")
    full_file = merge_parts(OUTPUT_DIR, run_id)

    print("\n" + "=" * 60)
    print("  GOTOVO!")
    if full_file:
        print(f"  Itogovyj fajl: {full_file}")
    print("  Kak otkryt v Google Tablicah:")
    print("  1. sheets.google.com")
    print("  2. Fajl -> Import -> Zagruzit fajl")
    print("  3. Razdelitel: zapyataya | Kodirovka: UTF-8")
    print("=" * 60)


if __name__ == "__main__":
    main()