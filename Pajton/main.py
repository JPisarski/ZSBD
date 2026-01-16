import csv
import cx_Oracle
from datetime import datetime
from time import sleep


def connect_to_db():
    dsn_tns = cx_Oracle.makedsn('213.184.8.44', '1521', service_name='orcl')  # Zmień na dane swojej bazy
    conn = cx_Oracle.connect(user='pisarskij', password='pisarski2025', dsn=dsn_tns)
    return conn

def validate_data(dane, table_name):
    errors = []

    if table_name == "GROBY":
        if not isinstance(dane.get('lokalizacja'), str) or len(dane.get('lokalizacja')) == 0:
            errors.append("lokalizacja musi być niepustym ciągiem znaków.")
        if not isinstance(dane.get('typ'), str) or len(dane.get('typ')) == 0:
            errors.append("typ musi być niepustym ciągiem znaków.")

        try:
            data_zarezerwowania = dane.get('data_zarezerwowania')
            if isinstance(data_zarezerwowania, str):
                data_zarezerwowania = datetime.strptime(data_zarezerwowania, '%Y-%m-%d')
        except ValueError:
            errors.append("data_zarezerwowania musi być poprawną datą w formacie YYYY-MM-DD.")

        try:
            data_konca_rezerwacji = dane.get('data_konca_rezerwacji')
            if isinstance(data_konca_rezerwacji, str):
                data_konca_rezerwacji = datetime.strptime(data_konca_rezerwacji, '%Y-%m-%d')
        except ValueError:
            errors.append("data_konca_rezerwacji musi być poprawną datą w formacie YYYY-MM-DD.")

        if not isinstance(dane.get('status'), str):
            errors.append("status musi być ciągiem znaków.")

    elif table_name == "ZMARLI":
        if not isinstance(dane.get('imie'), str) or len(dane.get('imie')) == 0:
            errors.append("imie musi być niepustym ciągiem znaków.")
        if not isinstance(dane.get('nazwisko'), str) or len(dane.get('nazwisko')) == 0:
            errors.append("nazwisko musi być niepustym ciągiem znaków.")

        try:
            data_urodzenia = dane.get('data_urodzenia')
            if isinstance(data_urodzenia, str):
                data_urodzenia = datetime.strptime(data_urodzenia, '%Y-%m-%d')
        except ValueError:
            errors.append("data_urodzenia musi być poprawną datą w formacie YYYY-MM-DD.")

        try:
            data_zgonu = dane.get('data_zgonu')
            if isinstance(data_zgonu, str):
                data_zgonu = datetime.strptime(data_zgonu, '%Y-%m-%d')
        except ValueError:
            errors.append("data_zgonu musi być poprawną datą w formacie YYYY-MM-DD.")

        try:
            grob_id = int(dane.get('grob_id'))
        except ValueError:
            errors.append("grob_id musi być liczbą całkowitą, odnoszącą się do istniejącego grobu.")

    elif table_name == "OPLATY":
        try:
            kwota = float(dane.get('kwota'))
        except ValueError:
            errors.append("kwota musi być liczbą zmiennoprzecinkową.")

        try:
            data = dane.get('data')
            if isinstance(data, str):
                data = datetime.strptime(data, '%Y-%m-%d')
        except ValueError:
            errors.append("data musi być poprawną datą w formacie YYYY-MM-DD.")

        if not isinstance(dane.get('status'), str) or len(dane.get('status')) == 0:
            errors.append("status musi być niepustym ciągiem znaków.")

        try:
            grob_id = int(dane.get('grob_id'))
        except ValueError:
            errors.append("grob_id musi być liczbą całkowitą, odnoszącą się do istniejącego zmarłego.")

    elif table_name == "REZERWACJE":
        try:
            data_zarezerwowania = dane.get('data_zarezerwowania')
            if isinstance(data_zarezerwowania, str):
                data_zarezerwowania = datetime.strptime(data_zarezerwowania, '%Y-%m-%d')
        except ValueError:
            errors.append("data_zarezerwowania musi być poprawną datą w formacie YYYY-MM-DD.")

        try:
            poczatek_rezerwacji = dane.get('poczatek_rezerwacji')
            if isinstance(poczatek_rezerwacji, str):
                poczatek_rezerwacji = datetime.strptime(poczatek_rezerwacji, '%Y-%m-%d')
        except ValueError:
            errors.append("poczatek_rezerwacji musi być poprawną datą w formacie YYYY-MM-DD.")

        try:
            koniec_rezerwacji = dane.get('koniec_rezerwacji')
            if isinstance(koniec_rezerwacji, str):
                koniec_rezerwacji = datetime.strptime(koniec_rezerwacji, '%Y-%m-%d')
        except ValueError:
            errors.append("koniec_rezerwacji musi być poprawną datą w formacie YYYY-MM-DD.")

        try:
            grob_id = int(dane.get('grob_id'))
        except ValueError:
            errors.append("grob_id musi być liczbą całkowitą, odnoszącą się do istniejącego grobu.")

    elif table_name == "ARCHIWUM_ZMARLYCH":
        if not isinstance(dane.get('imie'), str) or len(dane.get('imie')) == 0:
            errors.append("imie musi być niepustym ciągiem znaków.")
        if not isinstance(dane.get('nazwisko'), str) or len(dane.get('nazwisko')) == 0:
            errors.append("nazwisko musi być niepustym ciągiem znaków.")

        try:
            data_urodzenia = dane.get('data_urodzenia')
            if isinstance(data_urodzenia, str):
                data_urodzenia = datetime.strptime(data_urodzenia, '%Y-%m-%d')
        except ValueError:
            errors.append("data_urodzenia musi być poprawną datą w formacie YYYY-MM-DD.")

        try:
            data_zgonu = dane.get('data_zgonu')
            if isinstance(data_zgonu, str):
                data_zgonu = datetime.strptime(data_zgonu, '%Y-%m-%d')
        except ValueError:
            errors.append("data_zgonu musi być poprawną datą w formacie YYYY-MM-DD.")

        try:
            data_zarchiwizowania = dane.get('data_zarchiwizowania')
            if isinstance(data_zarchiwizowania, str):
                data_zarchiwizowania = datetime.strptime(data_zarchiwizowania, '%Y-%m-%d')
        except ValueError:
            errors.append("data_zarchiwizowania musi być poprawną datą w formacie YYYY-MM-DD.")

        if dane.get('opis') and not isinstance(dane.get('opis'), str):
            errors.append("opis musi być ciągiem znaków.")

    elif table_name == "LOGI":
        try:
            data = dane.get('data')
            if isinstance(data, str):
                data = datetime.strptime(data, '%Y-%m-%d %H:%M:%S')
        except ValueError:
            errors.append("data musi być poprawnym znacznikiem czasu w formacie YYYY-MM-DD HH:MM:SS.")

        if not isinstance(dane.get('wydarzenie'), str) or len(dane.get('wydarzenie')) == 0:
            errors.append("wydarzenie musi być niepustym ciągiem znaków.")

        if not isinstance(dane.get('opis'), str) or len(dane.get('opis')) == 0:
            errors.append("opis musi być niepustym ciągiem znaków.")

        if not isinstance(dane.get('uzytkownik'), str) or len(dane.get('uzytkownik')) == 0:
            errors.append("uzytkownik musi być niepustym ciągiem znaków.")

    elif table_name == "UZYTKOWNICY":
        conn = connect_to_db()
        cursor = conn.cursor()
        cursor.execute(f"""
                        SELECT COUNT(*)
                        FROM UZYTKOWNICY
                        WHERE nazwa = :nazwa
                    """, {"nazwa": dane.get('nazwa')})
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        if result[0] > 0:
            errors.append("nazwa musi być unikalna.")
        if not isinstance(dane.get('nazwa'), str) or len(dane.get('nazwa')) == 0:
            errors.append("nazwa musi być niepustym ciągiem znaków.")

        if not isinstance(dane.get('haslo'), str) or len(dane.get('haslo')) == 0:
            errors.append("haslo musi być niepustym ciągiem znaków.")

        if not isinstance(dane.get('rola'), str) or len(dane.get('rola')) == 0:
            errors.append("rola musi być niepustym ciągiem znaków.")

    return errors

def load_data_from_csv(file_path, table_name, columns):
    conn = connect_to_db()
    cursor = conn.cursor()

    with open(file_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)

        for row in reader:
            column_values = {col: row[col] for col in columns}
            print(column_values)

            errors = validate_data(column_values, table_name)
            if errors:
                print(f"Dane niepoprawne - wiersz {row}: {errors}")
                continue

            if 'kwota' in columns:
                temp = row['kwota']
                column_values['kwota'] = float(temp)

            cursor.execute(f"""
                INSERT INTO {table_name} ({', '.join(columns)})
                VALUES ({', '.join([f':{col}' for col in columns])})
            """, column_values)

    conn.commit()
    cursor.close()
    conn.close()

def archiwizacja_zmarlych():
    conn = connect_to_db()
    cursor = conn.cursor()

    try:

        query = """
            SELECT zmarli.zmarly_id, zmarli.imie, zmarli.nazwisko, zmarli.data_urodzenia, 
                   zmarli.data_zgonu, groby.data_konca_rezerwacji
            FROM zmarli 
            JOIN groby ON zmarli.grob_id = groby.grob_id
            WHERE groby.data_konca_rezerwacji < CURRENT_DATE 
            AND groby.status = 'Zajęty'
        """
        cursor.execute(query)
        zmarlych_do_archiwizacji = cursor.fetchall()

        if zmarlych_do_archiwizacji:
            for zmarly in zmarlych_do_archiwizacji:
                cursor.execute("""
                    INSERT INTO archiwum_zmarlych 
                    (zmarly_id, imie, nazwisko, data_urodzenia, data_zgonu, data_zarchiwizowania, opis)
                    VALUES (:zmarly_id, :imie, :nazwisko, :data_urodzenia, :data_zgonu, CURRENT_TIMESTAMP, 'Zarchiwizowane po zakończeniu rezerwacji grobu')
                """, {
                    'zmarly_id': zmarly[0],
                    'imie': zmarly[1],
                    'nazwisko': zmarly[2],
                    'data_urodzenia': zmarly[3],
                    'data_zgonu': zmarly[4]
                })

                cursor.execute("""
                    DELETE FROM zmarli WHERE zmarly_id = :zmarly_id
                """, {'zmarly_id': zmarly[0]})

                cursor.execute("""
                    INSERT INTO logi (data, wydarzenie, opis, uzytkownik)
                    VALUES (CURRENT_TIMESTAMP, 'Archiwizacja zmarłego', 'Zmarły o id: ' || :zmarly_id || ' został usunięty z tabeli zmarli oraz przeniesiony do archiwum.' , 'system')
                """, {'zmarly_id': zmarly[0]})


            conn.commit()
            print(f"Zarchiwizowano {len(zmarlych_do_archiwizacji)} zmarłych.")
        else:
            print("Brak zmarłych do zarchiwizowania.")

    except Exception as e:
        conn.rollback()
        print(f"Błąd podczas archiwizacji zmarłych: {e}")

    finally:
        cursor.close()
        conn.close()

def load_all_data():

    load_data_from_csv('groby.csv', 'GROBY', ['lokalizacja', 'typ', 'data_zarezerwowania', 'data_konca_rezerwacji', 'status'])
    load_data_from_csv('zmarli.csv', 'ZMARLI', ['imie', 'nazwisko', 'data_urodzenia', 'data_zgonu', 'grob_id'])
    load_data_from_csv('oplaty.csv', 'OPLATY', ['kwota', 'data', 'typ', 'status', 'grob_id'])
    load_data_from_csv('rezerwacje.csv', 'REZERWACJE', ['data_zarezerwowania', 'poczatek_rezerwacji', 'koniec_rezerwacji', 'grob_id'])
    load_data_from_csv('archiwum_zmarlych.csv', 'ARCHIWUM_ZMARLYCH', ['zmarly_id', 'imie', 'nazwisko', 'data_urodzenia', 'data_zgonu', 'data_zarchiwizowania', 'opis'])
    load_data_from_csv('logi.csv', 'LOGI', ['data', 'wydarzenie', 'opis', 'uzytkownik'])
    load_data_from_csv('uzytkownicy.csv', 'UZYTKOWNICY', ['nazwa', 'haslo', 'rola'])

    # archiwizacja_zmarlych()

def zaladunek():

    try:
        load_all_data()
        print("Dane, które przeszły walidację, zostały załadowane pomyślnie.")
    except Exception as e:
        print(f"Błąd: {e}")

if __name__ == '__main__':
    while True:
        zaladunek()
        print("Zakończono operację. Następna próba załadunku danych do bazy za 24h.")
        sleep(86400)