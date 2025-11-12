from flask import Flask, request, jsonify
import json
import os
import time

app = Flask(__name__)

@app.route('/')
def home():
    return jsonify({
        "status": "OK", 
        "message": "Telegram Monitor Server is running!",
        "endpoints": {
            "webhook": "POST /webhook-test/Parser",
            "status": "GET /status"
        }
    })

@app.route('/status')
def status():
    return jsonify({"status": "running", "timestamp": time.time()})

@app.route('/webhook-test/Parser', methods=['POST', 'GET'])
def webhook_parser():
    """Webhook для приема данных от телеграм монитора"""
    try:
        print("🔔 Получен запрос на webhook")
        
        if request.method == 'GET':
            return jsonify({
                "status": "ready", 
                "message": "Webhook is waiting for POST data from Telegram monitor"
            })
        
        # Получаем JSON данные
        data = request.json
        print("✅ Получены данные:", data)
        
        # Сохраняем в файл для отладки
        os.makedirs('leads', exist_ok=True)
        filename = f"leads/lead_{int(time.time())}.json"
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        
        print(f"💾 Данные сохранены в: {filename}")
        
        return jsonify({
            "status": "success", 
            "message": "Data received and saved",
            "filename": filename,
            "received_keys": list(data.keys()) if data else []
        }), 200
        
    except Exception as e:
        print(f"❌ Ошибка в webhook: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8000))
    print(f"🚀 Запуск сервера на порту {port}")
    app.run(host='0.0.0.0', port=port, debug=False)