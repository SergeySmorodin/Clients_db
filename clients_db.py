import psycopg2

def create_db(conn):
    with conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS clients(
            id SERIAL PRIMARY KEY,
            first_name VARCHAR(50),
            last_name VARCHAR(50),
            email VARCHAR(100) UNIQUE
            );     
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS phones(
            id SERIAL PRIMARY KEY,
            client_id INTEGER REFERENCES clients(id) ON DELETE CASCADE,
            phone VARCHAR(15) UNIQUE
            );          
        """)

        conn.commit()


def add_client(conn, first_name, last_name, email, phones=None):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO clients(first_name, last_name, email)
            VALUES(%s, %s, %s) RETURNING id;   
        """, (first_name, last_name, email))
        client_id = cur.fetchone()[0]
        if phones:
            for phone in phones:
                add_phone(conn, client_id, phone)
        conn.commit()
        return client_id


def add_phone(conn, client_id, phone):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO phones(client_id, phone)
            VALUES(%s, %s);
            """, (client_id, phone))
        conn.commit()


def delete_phone(conn, client_id, phone):
    with conn.cursor() as cur:
        cur.execute("""
        DELETE FROM phones WHERE client_id = %s AND phone = %s;
        """, (client_id, phone))
        conn.commit()


def change_client(conn, client_id, first_name=None, last_name=None, email=None, phones=None):
    with conn.cursor() as cur:
        if first_name:
            cur.execute("""
            UPDATE clients SET first_name = %s WHERE id = %s;
            """, (first_name, client_id))
        if last_name:
            cur.execute("""
            UPDATE clients SET last_name = %s WHERE id = %s;
            """, (last_name, client_id))
        if email:
            cur.execute("""
            UPDATE clients SET email = %s WHERE id = %s;
            """, (email, client_id))
        if phones is not None:
            cur.execute("""DELETE FROM phones WHERE client_id = %s;
            """, (client_id,))
            for phone in phones:
                add_phone(conn, client_id, phone)
        conn.commit()


def find_client(conn, first_name=None, last_name=None, email=None, phone=None):
    with conn.cursor() as cur:
        query = """SELECT clients.id, first_name, last_name, email, ARRAY_AGG(phones.phone) 
        FROM clients LEFT JOIN phones ON clients.id = phones.client_id WHERE TRUE"""
        params = []
        if first_name:
            query += """ AND first_name ILIKE %s"""
            params.append(f'%{first_name}%')
        if last_name:
            query += """ AND first_name ILIKE %s"""
            params.append(f'%{last_name}%')
        if email:
            query += """ AND email ILIKE %s"""
            params.append(f'%{email}%')
        if phone:
            query += """ AND phones.phone ILIKE %s"""
            params.append(f'%{phone}%')
        query += """ GROUP BY clients.id;"""
        cur.execute(query, params)
        return cur.fetchall()


def delete_client(conn, client_id):
    with conn.cursor() as cur:
        cur.execute("""
        DELETE FROM clients WHERE id = %s;
        """, (client_id,))
        conn.commit()


def drop_table(conn, table_name):
    with conn.cursor() as cur:
        cur.execute(f"""
        DROP TABLE IF EXISTS {table_name}
        """)
        conn.commit()



if __name__ == "__main__":
    with psycopg2.connect(database='clients_db', user='postgres', password='1213') as conn:

        # drop_table(conn, 'phones')
        # drop_table(conn, 'clients')

        create_db(conn)

        # Добавление клиентов
        client_1 = add_client(conn, 'Сергей', 'Смородин', 'smorodin@mail.ru', ['9875643956', '9876543431'])
        client_2 = add_client(conn, 'Алексей', 'Субботин', 'alex@mail.ru')
        client_3 = add_client(conn, 'Дмитрий', 'Алексеев','d.alex@google.com',['9547366632'])

        # Добавление телефона
        add_phone(conn, client_2, '9031843845')

        # Изменение данных клиента
        change_client(conn, client_3, phones=['9547766677', '9874628467'])
        change_client(conn, client_2, email='subbotin@mail.ru')

        # Удаление телефона
        delete_phone(conn, client_1, '9875643956')

        # Поиск клиента
        clients = find_client(conn, first_name='Сергей')
        print(clients)

        # Поиск клиента с измененными данными
        clients = find_client(conn, email='subbotin@mail.ru')
        print(clients)

        # Удаление клиента
        delete_client(conn, client_3)

        # Проверка удаления клиента
        clients = find_client(conn, first_name="Дмитрий")
        print(clients)


    conn.close()


