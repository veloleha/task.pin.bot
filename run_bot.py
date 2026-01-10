import sys
import os

# Отключаем буферизацию вывода
sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', buffering=1)
sys.stderr = os.fdopen(sys.stderr.fileno(), 'w', buffering=1)

print("🔄 Запуск бота...")
print("=" * 50)

# Импортируем и запускаем основной бот
import asyncio
from bot import main

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("⏹️ Бот остановлен пользователем (Ctrl+C)")
