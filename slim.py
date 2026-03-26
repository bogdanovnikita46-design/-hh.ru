# slim.py
# Берёт большой FULL_*.csv и делает лёгкую версию только с нужными колонками.
#
# Запуск:
#   python slim.py
#
# Положи slim.py в ту же папку hh_parser, рядом с папкой output/

import pandas as pd
import glob
import os

# ── Найти последний FULL файл в папке output ──────────────────
files = sorted(glob.glob(os.path.join("output", "FULL_*.csv")))
if not files:
    print("Файл FULL_*.csv не найден в папке output/")
    exit()

input_file = files[-1]  # берём самый свежий
print(f"Читаю: {input_file}")

# ── Колонки которые нужны для исследования ────────────────────
KEEP = [
    "id",
    "name",                                  # название вакансии
    "block",                                 # направление (Design, Journalism и т.д.)
    "search_query",                          # ключевое слово поиска
    "employer_name",                         # название компании
    "area_name",                             # регион
    "experience_name",                       # опыт (нет опыта / 1-3 года / 3-6 лет)
    "employment_name",                       # тип занятости
    "salary_from",                           # зарплата от
    "salary_to",                             # зарплата до
    "salary_currency",                       # валюта
    "education_id",                          # КОД требования к образованию ← главное
    "education_name",                        # НАЗВАНИЕ требования к образованию ← главное
    "upomyanuto_vysshee_v_tekste",           # упомянуто высшее в тексте True/False
    "obrazovanie_ne_trebuetsya_v_tekste",    # образование не требуется True/False
    "upomyanuto_portfolio",                  # упомянуто портфолио True/False
    "upomyanut_opyt_v_tekste",               # упомянут опыт True/False
    "key_skills_flat",                       # навыки через запятую
    "prof_role_name",                        # профессиональная роль
    "published_at",                          # дата публикации
    "alternate_url",                         # ссылка на вакансию
]

# ── Читаем только нужные колонки сразу (не грузим всё в память) ──
print("Читаю только нужные колонки...")
df = pd.read_csv(
    input_file,
    usecols=lambda c: c in KEEP,  # берём только колонки из списка KEEP
    dtype={"id": str},
    low_memory=False,
)

# Упорядочиваем колонки как в KEEP (те что нашлись)
existing = [c for c in KEEP if c in df.columns]
df = df[existing]

print(f"Строк: {len(df)} | Колонок: {len(df.columns)}")

# ── Сохраняем ─────────────────────────────────────────────────
output_file = input_file.replace("FULL_", "SLIM_")
df.to_csv(output_file, index=False, encoding="utf-8-sig")

size_mb = os.path.getsize(output_file) / 1024 / 1024
print(f"\nГотово! Файл: {output_file}")
print(f"Размер: {size_mb:.1f} МБ")

if size_mb < 5:
    print("Можно загружать в Google Таблицы напрямую!")
elif size_mb < 100:
    print("Для Google Таблиц всё ещё великоват.")
    print("Запусти python slim.py --sample чтобы взять случайную выборку 5000 строк.")