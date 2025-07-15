import os
import csv
import random
import string
import pandas as pd
import logging
from datetime import datetime
from collections import Counter

# ======= ПАРАМЕТРЫ ==========
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
# ============================

def get_timestamp(fmt="%Y%m%d_%H%M%S"):
    return datetime.now().strftime(fmt)

def get_log_file():
    date_str = datetime.now().strftime("%Y-%m-%d")
    filename = f"{PARAMS['log_base']}_{date_str}.log"
    return os.path.join(PARAMS['log_dir'], filename)

def setup_logging():
    os.makedirs(PARAMS['log_dir'], exist_ok=True)
    logger = logging.getLogger()
    logger.handlers.clear()
    logger.setLevel(logging.INFO)
    # Файл
    file_handler = logging.FileHandler(get_log_file(), encoding='utf-8')
    file_handler.setLevel(logging.INFO)
    # Консоль
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.INFO)
    # Формат логов
    fmt = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
    file_handler.setFormatter(fmt)
    stream_handler.setFormatter(fmt)
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    logger.info("=== START RUN ===")
    logger.info("Параметры запуска: %s", PARAMS)
    return logger

logger = setup_logging()

def load_org_units(path):
    try:
        df = pd.read_csv(path, sep=';', dtype=str)
        units = df[['TB_CODE','TB_FULL_NAME','TB_SHORT_NAME','GOSB_CODE','GOSB_NAME','GOSB_SHORT_NAME','ORG_UNIT_CODE']].to_dict('records')
        logger.info(f"Загружено подразделений: {len(units)} из {path}")
        return units
    except Exception as e:
        logger.error(f"Ошибка при загрузке оргструктуры: {e}")
        raise

# --- ФИО ---
LAST_NAMES_MALE = [
    "Иванов", "Петров", "Сидоров", "Кузнецов", "Попов", "Васильев", "Смирнов", "Михайлов", "Новиков", "Федоров",
    "Белов", "Семенов", "Егоров", "Козлов", "Соловьев", "Калинин", "Тихонов", "Жуков", "Орлов", "Макаров", "Чернов"
]
LAST_NAMES_FEMALE = [x + "а" if not x.endswith("в") else x[:-1] + "ва" for x in LAST_NAMES_MALE]
FIRST_NAMES_MALE = [
    "Алексей", "Иван", "Дмитрий", "Сергей", "Виктор", "Владимир", "Павел", "Егор", "Константин", "Георгий",
    "Максим", "Андрей", "Артем", "Никита", "Роман", "Олег", "Ярослав", "Денис", "Станислав", "Виталий", "Юрий"
]
FIRST_NAMES_FEMALE = [
    "Елена", "Мария", "Анна", "Ирина", "Ольга", "Наталья", "Светлана", "Татьяна", "Алина", "Екатерина",
    "Дарья", "Юлия", "Кристина", "Марина", "Валерия", "Виктория", "Полина", "Вероника", "Евгения", "София", "Алёна"
]
MIDDLE_NAMES_MALE = [
    "Алексеевич", "Иванович", "Дмитриевич", "Сергеевич", "Викторович", "Владимирович", "Павлович", "Егорович",
    "Константинович", "Георгиевич", "Максимович", "Андреевич", "Артемович", "Никитович", "Романович", "Олегович",
    "Ярославович", "Денисович", "Станиславович", "Витальевич", "Юрьевич"
]
MIDDLE_NAMES_FEMALE = [x[:-2] + "вна" for x in MIDDLE_NAMES_MALE]

def generate_unique_fio_set(count):
    fio_set = set()
    results = []
    gender_list = []
    male_count = count // 2
    female_count = count - male_count

    logger.info(f"Планируется мужчин: {male_count}, женщин: {female_count}")

    # Генерируем мужчин
    for _ in range(male_count):
        tries = 0
        while True:
            ln = random.choice(LAST_NAMES_MALE)
            fn = random.choice(FIRST_NAMES_MALE)
            mn = random.choice(MIDDLE_NAMES_MALE)
            fio = f"{ln} {fn} {mn}"
            if fio not in fio_set:
                fio_set.add(fio)
                results.append((ln, fn, mn))
                gender_list.append("M")
                break
            tries += 1
            if tries > 1000:
                logger.warning("Проблема с уникальностью мужских ФИО, расширьте список.")
                break

    # Генерируем женщин
    for _ in range(female_count):
        tries = 0
        while True:
            ln = random.choice(LAST_NAMES_FEMALE)
            fn = random.choice(FIRST_NAMES_FEMALE)
            mn = random.choice(MIDDLE_NAMES_FEMALE)
            fio = f"{ln} {fn} {mn}"
            if fio not in fio_set:
                fio_set.add(fio)
                results.append((ln, fn, mn))
                gender_list.append("F")
                break
            tries += 1
            if tries > 1000:
                logger.warning("Проблема с уникальностью женских ФИО, расширьте список.")
                break

    # Если сотрудников больше, чем вариантов, будем добавлять рандомные буквы
    while len(results) < count:
        gender = random.choice(["M", "F"])
        if gender == "M":
            ln = random.choice(LAST_NAMES_MALE) + random.choice(string.ascii_uppercase)
            fn = random.choice(FIRST_NAMES_MALE) + random.choice(string.ascii_uppercase)
            mn = random.choice(MIDDLE_NAMES_MALE)
        else:
            ln = random.choice(LAST_NAMES_FEMALE) + random.choice(string.ascii_uppercase)
            fn = random.choice(FIRST_NAMES_FEMALE) + random.choice(string.ascii_uppercase)
            mn = random.choice(MIDDLE_NAMES_FEMALE)
        fio = f"{ln} {fn} {mn}"
        if fio not in fio_set:
            fio_set.add(fio)
            results.append((ln, fn, mn))
            gender_list.append(gender)
    logger.info(f"Итого уникальных ФИО: {len(results)} (мужчин: {gender_list.count('M')}, женщин: {gender_list.count('F')})")
    return results, gender_list

def generate_unique_tn_set(count):
    numbers = set()
    while len(numbers) < count:
        num = str(random.randint(10**3, 10**10-1)).zfill(10)
        numbers.add(num)
    logger.info(f"Итого уникальных табельных номеров: {len(numbers)}")
    return list(numbers)

def distribute_users(units, n_users):
    n_units = len(units)
    base = n_users // n_units
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
    for i in range(rest):
        result.append(i % n_units)
    random.shuffle(result)
    logger.info(f"Распределено сотрудников по подразделениям: {Counter(result)}")
    return result

def generate_employees(org_units, n_users, role_distribution):
    fio_list, gender_list = generate_unique_fio_set(n_users)
    tn_list = generate_unique_tn_set(n_users)
    unit_indices = distribute_users(org_units, n_users)

    roles = []
    total_roles = sum(role_distribution.values())
    for role, cnt in role_distribution.items():
        roles.extend([role]*cnt)
    if len(roles) < n_users:
        roles.extend(["OTHER"]*(n_users-len(roles)))
    elif len(roles) > n_users:
        roles = roles[:n_users]
    random.shuffle(roles)

    employees = []
    for idx in range(n_users):
        org = org_units[unit_indices[idx]]
        ln, fn, mn = fio_list[idx]
        tn = tn_list[idx]
        role = roles[idx]
        gender = gender_list[idx]
        emp = {
            "TN": tn,
            "LastName": ln,
            "FirstName": fn,
            "MidleName": mn,
            "Gender": gender,
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
    logger.info(f"Распределено по ролям: {Counter(roles)}")
    return employees

def save_employees(employees, out_dir, out_base):
    os.makedirs(out_dir, exist_ok=True)
    ts = get_timestamp()
    csv_name = f"{out_base}_{ts}.csv"
    xlsx_name = f"{out_base}_{ts}.xlsx"
    csv_path = os.path.join(out_dir, csv_name)
    xlsx_path = os.path.join(out_dir, xlsx_name)

    fields = list(employees[0].keys())
    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, delimiter=";")
        writer.writeheader()
        writer.writerows(employees)
    logger.info(f"Данные сотрудников сохранены в CSV: {csv_path}")

    df = pd.DataFrame(employees)
    with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="GEN_USER", index=False)
    logger.info(f"Данные сотрудников сохранены в Excel: {xlsx_path}")

def main():
    logger.info("Начало работы программы")
    org_path = os.path.join(PARAMS["input_dir"], PARAMS["org_unit_file"])
    org_units = load_org_units(org_path)
    employees = generate_employees(org_units, PARAMS["user_count"], PARAMS["role_distribution"])
    logger.info(f"Сгенерировано сотрудников: {len(employees)}")
    save_employees(employees, PARAMS["output_dir"], PARAMS["output_base"])
    logger.info("Генерация завершена успешно")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.exception(f"Ошибка при выполнении: {e}")
        raise
