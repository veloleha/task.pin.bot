"""
Скрипт миграции базы данных для добавления колонки message_id
"""
import sqlite3

DB_NAME = "tasks.db"

def migrate():
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    
    try:
        # Проверяем, существует ли колонка message_id
        c.execute("PRAGMA table_info(tasks)")
        columns = [column[1] for column in c.fetchall()]
        
        if 'message_id' not in columns:
            print("⚙️ Добавляю колонку message_id в таблицу tasks...")
            c.execute("ALTER TABLE tasks ADD COLUMN message_id INTEGER")
            conn.commit()
            print("✅ Миграция выполнена успешно!")
        else:
            print("✅ Колонка message_id уже существует, миграция не требуется")
    
    except Exception as e:
        print(f"❌ Ошибка миграции: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    migrate()
