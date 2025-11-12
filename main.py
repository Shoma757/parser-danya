import asyncio
import pandas as pd
from telethon import TelegramClient
import time
import aiohttp
import json
import re

print("=== ТЕЛЕГРАМ МОНИТОРИНГ ДОПУСКОВ ===")

async def send_to_webhook(lead_data, webhook_url):
    """Отправляет лид в webhook"""
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                webhook_url,
                json=lead_data,
                headers={'Content-Type': 'application/json'},
                timeout=10
            ) as response:
                if response.status == 200:
                    print(f"Лид отправлен: {lead_data['keywords']}")
                    return True
                else:
                    print(f"Ошибка отправки: {response.status}")
                    return False
    except Exception as e:
        print(f"Ошибка webhook: {e}")
        return False

def clean_group_link(link):
    """Очищает и преобразует ссылку в правильный формат"""
    if not link or pd.isna(link):
        return None
    
    link = str(link).strip()
    
    # Если это числовой ID (отрицательный для групп)
    if link.replace('-', '').isdigit():
        num_id = int(link)
        # Для групп ID должен быть отрицательным с -100 префиксом
        if num_id < 0 and abs(num_id) > 1000000000:
            return int(link)
        elif num_id > 0:
            return int(f"-100{num_id}")
        else:
            return int(link)
    
    # Удаляем ссылки на конкретные сообщения
    if '/-' in link or re.search(r'/\d+$', link):
        link = link.split('/')[-2] if '/' in link else link
    
    # Очищаем обычные ссылки
    if 't.me/' in link:
        username = link.split('t.me/')[-1].split('/')[0]
        if username:
            return f"@{username}" if not username.startswith('@') else username
    
    if link.startswith('@'):
        return link
    
    return link

async def safe_get_entity(client, identifier):
    """Безопасное получение сущности с обработкой ошибок"""
    try:
        return await client.get_entity(identifier)
    except Exception as e:
        print(f"Не удалось получить группу {identifier}: {e}")
        return None

def get_message_url(group, message_id, group_link):
    """Формирует ссылку на сообщение"""
    try:
        if isinstance(group_link, str) and group_link.startswith('@'):
            # Для публичных групп с username
            return f"https://t.me/{group_link[1:]}/{message_id}"
        else:
            # Для приватных групп по ID
            group_id = getattr(group, 'id', None)
            if group_id:
                # Преобразуем ID для ссылки (убираем -100 префикс если есть)
                if str(group_id).startswith('-100'):
                    channel_id = str(group_id)[4:]
                else:
                    channel_id = str(group_id).replace('-', '')
                return f"https://t.me/c/{channel_id}/{message_id}"
    except Exception as e:
        print(f"Не удалось сформировать ссылку: {e}")
    
    return "Недоступно"

def get_user_info(msg):
    """Извлекает информацию о пользователе из сообщения"""
    try:
        sender = msg.sender
        if sender:
            username = getattr(sender, 'username', None)
            first_name = getattr(sender, 'first_name', '')
            last_name = getattr(sender, 'last_name', '')
            user_id = getattr(sender, 'id', None)
            
            full_name = f"{first_name} {last_name}".strip()
            
            return {
                "username": f"@{username}" if username else None,
                "user_id": user_id,
                "full_name": full_name if full_name else None
            }
    except Exception as e:
        print(f"Не удалось получить информацию о пользователе: {e}")
    
    return {
        "username": None,
        "user_id": None,
        "full_name": None
    }

async def main():
    # ВАШИ ДАННЫЕ TELEGRAM
    api_id = 14535587
    api_hash = '007b2bc4ed88c84167257c4a57dd3e75'
    phone = '+77762292659'
    
    # WEBHOOK URL
    webhook_url = "https://primary-production-9c67.up.railway.app/webhook-test/Parser"
    
    try:
        print("Подключаемся к Telegram...")
        client = TelegramClient('session', api_id, api_hash)
        await client.start(phone=phone)
        
        me = await client.get_me()
        print(f"Авторизован как: {me.first_name} (@{me.username})")
        print(f"Webhook: {webhook_url}")
        print("Автоматическое вступление в группы ОТКЛЮЧЕНО")
        print("РЕЖИМ: 1 сообщение/10 сек, 10 групп -> 5 мин отдых")
        print("БЕСКОНЕЧНЫЙ ЦИКЛ - никогда не выключается")
        
        # Загружаем и очищаем группы из Excel
        try:
            df = pd.read_excel('bot1.xlsx')
            
            # Автоматически находим колонку с группами
            group_column = None
            for col in df.columns:
                if any(keyword in col.lower() for keyword in ['group', 'link', 'url', 'username', 'id']):
                    group_column = col
                    break
            
            if not group_column:
                group_column = df.columns[0]
            
            # Очищаем ссылки
            raw_groups = df[group_column].dropna().tolist()
            groups = []
            
            for link in raw_groups:
                cleaned = clean_group_link(link)
                if cleaned and cleaned not in groups:
                    groups.append(cleaned)
            
            print(f"Загружено групп: {len(groups)}")
            print(f"Примеры: {groups[:3]}...")
            
        except Exception as e:
            print(f"Ошибка загрузки Excel: {e}")
            groups = []
        
        # КЛЮЧЕВЫЕ СЛОВА ДЛЯ ПОИСКА
        keywords = [
            "получить допуск для рабочих", "рабочий допуск на виллу", "пасс для рабочих", 
            "пасс для работ на квартире", "пасс для работ на вилле", "пропуск для рабочих",
            "пропуск для рабочих на квартиру", "пропуск для рабочих на виллу", "разрешение на работы", 
            "разрешение на работы от УК", "разрешение на работы от комьюнити менеджмента", 
            "разрешение на работы от билдинга", "разрешение на работы от билдинг менеджмента",
            # Дополнительные варианты
            "допуск для рабочих", "рабочий пропуск", "пропуск для ремонтников",
            "разрешение на ремонт", "допуск на объект", "пропуск на виллу",
            "пропуск в билдинг", "допуск в билдинг", "рабочий пасс",
            "оформить пропуск", "получить пропуск", "нужен допуск"
        ]
        
        print(f"Загружено ключевых слов: {len(keywords)}")
        print("Ищем:")
        for i, keyword in enumerate(keywords[:8], 1):
            print(f"   {i}. {keyword}")
        if len(keywords) > 8:
            print(f"   ... и еще {len(keywords) - 8} слов")
        
        # БЕСКОНЕЧНЫЙ ЦИКЛ МОНИТОРИНГА
        total_cycles = 0
        total_leads_found = 0
        total_messages_checked = 0
        processed_messages = set()  # Для отслеживания уже обработанных сообщений
        
        while True:
            total_cycles += 1
            print(f"\n{'='*60}")
            print(f"ЦИКЛ {total_cycles} - {time.strftime('%H:%M:%S')}")
            print(f"Всего лидов найдено: {total_leads_found}")
            print(f"Проверяем {len(groups)} групп...")
            print(f"{'='*60}")
            
            groups_processed = 0
            leads_in_cycle = 0
            messages_in_cycle = 0
            
            for group_link in groups:
                try:
                    groups_processed += 1
                    print(f"[{groups_processed}/{len(groups)}] Группа: {group_link}")
                    
                    group = await safe_get_entity(client, group_link)
                    if not group:
                        print("Группа недоступна, пропускаем...")
                        await asyncio.sleep(2)
                        continue
                    
                    group_name = getattr(group, 'title', str(group_link))
                    group_id = getattr(group, 'id', 'unknown')
                    
                    # Получаем последние 3 сообщения
                    messages = await client.get_messages(group, limit=3)
                    
                    if messages:
                        group_messages_count = 0
                        for msg in messages:
                            # Создаем уникальный ID сообщения
                            message_id = f"{group_id}_{msg.id}"
                            
                            # Пропускаем уже обработанные сообщения
                            if message_id in processed_messages:
                                continue
                            
                            group_messages_count += 1
                            messages_in_cycle += 1
                            total_messages_checked += 1
                            
                            if msg.text:
                                text = msg.text.lower()
                                message_time = msg.date.strftime('%Y-%m-%d %H:%M:%S') if msg.date else "Неизвестно"
                                
                                # Получаем информацию о пользователе
                                user_info = get_user_info(msg)
                                
                                # Ищем ключевые слова
                                found_keywords = []
                                for keyword in keywords:
                                    if keyword in text:
                                        found_keywords.append(keyword)
                                
                                # Если нашли ключевые слова - отправляем в webhook
                                if found_keywords:
                                    print(f"НАЙДЕНО в '{group_name}': {', '.join(found_keywords)}")
                                    print(f"Сообщение: {msg.text[:100]}...")
                                    if user_info['username']:
                                        print(f"Автор: {user_info['username']}")
                                    
                                    # Формируем ссылку на сообщение
                                    message_url = get_message_url(group, msg.id, group_link)
                                    print(f"Ссылка на сообщение: {message_url}")
                                    
                                    # Формируем данные для webhook
                                    lead_data = {
                                        "keywords": found_keywords,
                                        "group_name": group_name,
                                        "group_link": str(group_link),
                                        "message_text": msg.text[:1000],
                                        "message_time": message_time,
                                        "found_time": time.strftime('%Y-%m-%d %H:%M:%S'),
                                        "message_id": msg.id,
                                        "group_id": group_id,
                                        "message_url": message_url,
                                        "user_username": user_info['username'],
                                        "user_id": user_info['user_id'],
                                        "user_full_name": user_info['full_name'],
                                        "source": "telegram_monitor",
                                        "cycle": total_cycles
                                    }
                                    
                                    # Отправляем в webhook
                                    success = await send_to_webhook(lead_data, webhook_url)
                                    if success:
                                        leads_in_cycle += 1
                                        total_leads_found += 1
                                    
                                    # Добавляем в обработанные
                                    processed_messages.add(message_id)
                                    
                                    # Пауза после нахождения лида
                                    await asyncio.sleep(2)
                            
                            # Добавляем сообщение в обработанные даже если ключевых слов нет
                            processed_messages.add(message_id)
                        
                        # Статистика по группе
                        if group_messages_count > 0:
                            print(f"В группе просмотрено {group_messages_count} сообщений")
                    
                    else:
                        print(f"В группе нет сообщений для проверки")
                    
                    # Пауза 10 секунд между группами
                    print("Ждем 10 секунд...")
                    await asyncio.sleep(10)
                    
                    # После каждых 10 групп - перерыв 5 минут
                    if groups_processed % 10 == 0 and groups_processed < len(groups):
                        print(f"Обработано {groups_processed} групп - перерыв 5 минут...")
                        for i in range(5, 0, -1):
                            print(f"   Осталось: {i} минут")
                            await asyncio.sleep(60)
                        print("Продолжаем мониторинг...")
                    
                except Exception as e:
                    print(f"Ошибка в группе {group_link}: {e}")
                    # Пауза даже при ошибке
                    await asyncio.sleep(10)
                    continue
            
            # Статистика цикла
            print(f"ЦИКЛ {total_cycles} ЗАВЕРШЕН")
            print(f"Обработано групп: {groups_processed}")
            print(f"Просмотрено сообщений: {messages_in_cycle}")
            print(f"Найдено лидов: {leads_in_cycle}")
            print(f"Всего лидов: {total_leads_found}")
            print(f"Всего сообщений обработано: {total_messages_checked}")
            
            # Уведомление о результатах цикла
            if leads_in_cycle > 0:
                print(f"В этом цикле найдено {leads_in_cycle} лидов!")
            else:
                print(f"В этом цикле лидов не найдено")
            
            # Очищаем старые сообщения (чтобы не копить слишком много)
            if len(processed_messages) > 1000:
                processed_messages = set(list(processed_messages)[-500:])
                print("Очищена история обработанных сообщений")
            
            # Перерыв 5 минут после полного цикла
            print(f"Перерыв 5 минут до следующего цикла...")
            for i in range(5, 0, -1):
                print(f"   Следующий цикл через: {i} минут")
                await asyncio.sleep(60)
            
    except Exception as e:
        print(f"Критическая ошибка: {e}")
        print("Перезапуск через 1 минуту...")
        await asyncio.sleep(60)
        # Автоматический перезапуск
        await main()

# Бесконечный запуск
print("Запускаем телеграм монитор...")
while True:
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Остановлено пользователем")
        break
    except Exception as e:
        print(f"Фатальная ошибка: {e}")
        print("Перезапуск через 30 секунд...")
        time.sleep(30)