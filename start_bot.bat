@echo off
chcp 65001 >nul
echo ========================================
echo    TaskPinBot Launcher
echo ========================================
echo.

REM Проверка наличия .env файла
if not exist .env (
    echo [ERROR] Файл .env не найден!
    echo Создайте файл .env с токеном бота.
    echo.
    pause
    exit /b 1
)

REM Проверка установки зависимостей
echo [INFO] Проверка зависимостей...
pip show aiogram >nul 2>&1
if errorlevel 1 (
    echo [WARN] Зависимости не установлены. Устанавливаю...
    pip install -r requirements.txt
    if errorlevel 1 (
        echo [ERROR] Ошибка установки зависимостей!
        pause
        exit /b 1
    )
)

echo [INFO] Запуск бота...
echo.
python bot.py

if errorlevel 1 (
    echo.
    echo [ERROR] Бот завершился с ошибкой!
    pause
    exit /b 1
)

pause
