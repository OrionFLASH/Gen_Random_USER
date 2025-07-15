PARAMS = {
    "input_dir": "//Users//orionflash//Desktop//MyProject//Gen_Random_USER//IN_BASE",
    "org_unit_file": "ORG_UNIT.csv",
    "output_dir": "//Users//orionflash//Desktop//MyProject//Gen_Random_USER//OUT_CSV",
    "output_base": "EMPLOEE",
    "log_dir": "//Users//orionflash//Desktop//MyProject//Gen_Random_USER//LOGS",
    "log_base": "LOG",
    "user_count": 8000,
    "role_distribution": {
        "KM_KKSB": 1700,
        "KM_MNS": 200,
        "RUK_KKSB": 320,
        "RUK_MNS": 15,
        "RUK_TB": 30,
        "RUK_CA": 10,
        "DRKB": 50,
        "KM_SB1": 2000,
        "RUK_SB1": 200,
        "UMB1": 500,
        "RUK_UMB1": 10,
        "OTHER_CA": 30,
        "SERVISE_MAN": 1700
        # "OTHER" - автоматически рассчитывается
    }
}

import os
import csv
import random
import string
import pandas as pd
import logging
from datetime import datetime
from collections import Counter

# ======= ПАРАМЕТРЫ (РЕДАКТИРУЕМЫЕ) ==========
PARAMS = {
    "input_dir": "//Users//orionflash//Desktop//MyProject//Gen_Random_USER//IN_BASE",
    "org_unit_file": "ORG_UNIT.csv",
    "output_dir": "//Users//orionflash//Desktop//MyProject//Gen_Random_USER//OUT_CSV",
    "output_base": "EMPLOEE",
    "log_dir": "//Users//orionflash//Desktop//MyProject//Gen_Random_USER//LOGS",
    "log_base": "LOG",
    "user_count": 8000,
    "role_distribution": {
        "KM_KKSB": 1700,
        "KM_MNS": 200,
        "RUK_KKSB": 320,
        "RUK_MNS": 15,
        "RUK_TB": 30,
        "RUK_CA": 10,
        "DRKB": 50,
        "KM_SB1": 2000,
        "RUK_SB1": 200,
        "UMB1": 500,
        "RUK_UMB1": 10,
        "OTHER_CA": 30,
        "SERVISE_MAN": 1700
    }
}
# ============================================

# --- Подготовка путей и логов ---
def get_timestamp(fmt="%Y%m%d_%H%M%S"):
    return datetime.now().strftime(fmt)

def get_log_file():
    date_str = datetime.now().strftime("%Y-%m-%d")
    filename = f"{PARAMS['log_base']}_{date_str}.log"
    return os.path.join(PARAMS['log_dir'], filename)

def setup_logging():
    os.makedirs(PARAMS['log_dir'], exist_ok=True)
    logging.basicConfig(
        filename=get_log_file(),
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s'
    )
    logging.info("=== START RUN ===")

setup_logging()

# --- Загрузка подразделений ---
def load_org_units(path):
    try:
        df = pd.read_csv(path, sep=';', dtype=str)
        units = df[['TB_CODE','TB_FULL_NAME','TB_SHORT_NAME','GOSB_CODE','GOSB_NAME','GOSB_SHORT_NAME','ORG_UNIT_CODE']].to_dict('records')
        logging.info(f"Загружено подразделений: {len(units)}")
        return units
    except Exception as e:
        logging.error(f"Ошибка при загрузке оргструктуры: {e}")
        raise

# --- Генерация ФИО ---
LAST_NAMES = [
    "Иванов", "Петров", "Сидоров", "Кузнецов", "Попов", "Васильев", "Смирнов", "Михайлов", "Новиков", "Федоров",
    "Белов", "Семенов", "Егоров", "Козлов", "Соловьев", "Калинин", "Тихонов", "Жуков", "Орлов", "Макаров", "Чернов"
]
FIRST_NAMES = [
    "Алексей", "Иван", "Дмитрий", "Сергей", "Виктор", "Владимир", "Павел", "Егор", "Константин", "Георгий",
    "Максим", "Андрей", "Артем", "Никита", "Роман", "Олег", "Ярослав", "Денис", "Станислав", "Виталий", "Юрий"
]
MIDDLE_NAMES = [
    "Алексеевич", "Иванович", "Дмитриевич", "Сергеевич", "Викторович", "Владимирович", "Павлович", "Егорович",
    "Константинович", "Георгиевич", "Максимович", "Андреевич", "Артемович", "Никитович", "Романович", "Олегович",
    "Ярославович", "Денисович", "Станиславович", "Витальевич", "Юрьевич"
]

def generate_unique_fio_set(count):
    fio_set = set()
    results = []
    # Пытаемся генерировать случайные ФИО, чтобы не было полных совпадений
    while len(results) < count:
        ln = random.choice(LAST_NAMES)
        fn = random.choice(FIRST_NAMES)
        mn = random.choice(MIDDLE_NAMES)
        fio = f"{ln} {fn} {mn}"
        if fio not in fio_set:
            fio_set.add(fio)
            results.append((ln, fn, mn))
        # При большом числе сотрудников нужен список ФИО больше, чем стандартные 20 фамилий!
        if len(fio_set) < count and len(fio_set) > 0.85 * (len(LAST_NAMES)*len(FIRST_NAMES)*len(MIDDLE_NAMES)):
            # Добавить новые имена/фамилии динамически
            LAST_NAMES.append(ln + random.choice(string.ascii_uppercase))
            FIRST_NAMES.append(fn + random.choice(string.ascii_uppercase))
            MIDDLE_NAMES.append(mn + random.choice(string.ascii_uppercase))
    return results

# --- Генерация уникального табельного номера ---
def generate_unique_tn_set(count):
    numbers = set()
    while len(numbers) < count:
        num = str(random.randint(10**3, 10**10-1)).zfill(10)
        numbers.add(num)
    return list(numbers)

# --- Распределение по подразделениям ---
def distribute_users(units, n_users):
    """ Вернет список индексов подразделения для каждого сотрудника """
    n_units = len(units)
    base = n_users // n_units
    # В каждое подразделение — базовое количество ±20%, кроме нескольких (1-2 по 1-2 чел)
    result = []
    rest = n_users
    for i, u in enumerate(units):
        if i < n_units - 2:
            cnt = base + random.randint(-int(0.2*base), int(0.2*base))
        else:
            cnt = random.randint(1,2)
        if cnt > rest:
            cnt = rest
        result.extend([i]*cnt)
        rest -= cnt
    # Если остались сотрудники, распределить по первым подразделениям
    for i in range(rest):
        result.append(i % n_units)
    random.shuffle(result)
    return result

# --- Генерация сотрудников ---
def generate_employees(org_units, n_users, role_distribution):
    # Генерируем ФИО и ТН
    fio_list = generate_unique_fio_set(n_users)
    tn_list = generate_unique_tn_set(n_users)
    # Распределяем подразделения
    unit_indices = distribute_users(org_units, n_users)
    # Генерируем роли
    roles = []
    total_roles = sum(role_distribution.values())
    for role, cnt in role_distribution.items():
        roles.extend([role]*cnt)
    # Остальные — "OTHER"
    if len(roles) < n_users:
        roles.extend(["OTHER"]*(n_users-len(roles)))
    elif len(roles) > n_users:
        roles = roles[:n_users]
    random.shuffle(roles)

    # Формируем сотрудников
    employees = []
    for idx in range(n_users):
        org = org_units[unit_indices[idx]]
        ln, fn, mn = fio_list[idx]
        tn = tn_list[idx]
        role = roles[idx]
        emp = {
            "TN": tn,
            "LastName": ln,
            "FirstName": fn,
            "MidleName": mn,
            "ROLE_CODE": role,
            "TB_CODE": org["TB_CODE"],
            "TB_FULL_NAME": org["TB_FULL_NAME"],
            "TB_SHORT_NAME": org["TB_SHORT_NAME"],
            "GOSB_CODE": org["GOSB_CODE"],
            "GOSB_NAME": org["GOSB_NAME"],
            "GOSB_SHORT_NAME": org["GOSB_SHORT_NAME"],
            "ORG_UNIT_CODE": org["ORG_UNIT_CODE"]
        }
        employees.append(emp)
    return employees

# --- Сохранение в CSV и Excel ---
def save_employees(employees, out_dir, out_base):
    os.makedirs(out_dir, exist_ok=True)
    ts = get_timestamp()
    csv_name = f"{out_base}_{ts}.csv"
    xlsx_name = f"{out_base}_{ts}.xlsx"
    csv_path = os.path.join(out_dir, csv_name)
    xlsx_path = os.path.join(out_dir, xlsx_name)

    fields = list(employees[0].keys())
    # CSV
    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, delimiter=";")
        writer.writeheader()
        writer.writerows(employees)
    logging.info(f"Данные сотрудников сохранены в CSV: {csv_path}")

    # Excel
    df = pd.DataFrame(employees)
    with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="GEN_USER", index=False)
    logging.info(f"Данные сотрудников сохранены в Excel: {xlsx_path}")

def main():
    # 1. Загрузка подразделений
    org_path = os.path.join(PARAMS["input_dir"], PARAMS["org_unit_file"])
    org_units = load_org_units(org_path)

    # 2. Генерация сотрудников
    employees = generate_employees(org_units, PARAMS["user_count"], PARAMS["role_distribution"])
    logging.info(f"Сгенерировано сотрудников: {len(employees)}")

    # 3. Сохранение
    save_employees(employees, PARAMS["output_dir"], PARAMS["output_base"])
    logging.info("Завершено без ошибок.")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.exception(f"Ошибка при выполнении: {e}")
        raise

