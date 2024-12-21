import datetime
from flask import Flask, request, render_template_string, jsonify
from collections import defaultdict
import json
import os
from urllib.parse import quote, unquote
import snappy
import struct

# Создаем экземпляр Flask приложения
app = Flask(__name__)


def format_odds_data(data):
    """
    Форматирует данные коэффициентов для отображения на веб-странице.

    :param data: Словарь с данными матча и исходов
    :return: Словарь с отформатированными данными или сообщение об ошибке
    """
    try:
        # Инициализируем структуру для хранения отформатированных данных
        formatted_data = {
            'Match': defaultdict(list),
            '1H': defaultdict(list),
            '2H': defaultdict(list)
        }

        # Проходим по всем исходам и распределяем их по периодам и типам ставок
        for outcome in data['outcomes']:
            period = 'Match'  # По умолчанию период "Матч"
            # Определяем период на основе имени типа исхода
            if outcome['type_name'].startswith('1H'):
                period = '1H'
            elif outcome['type_name'].startswith('2H'):
                period = '2H'

            # Убираем обозначения периодов из имени типа ставки
            bet_type = outcome['type_name'].replace('1H', '').replace('2H', '')
            # Форматируем информацию об исходе
            formatted_outcome = f"{outcome['type']}: {outcome.get('line', 'N/A')} @ {outcome['odds']}"
            # Добавляем исход в соответствующую категорию
            formatted_data[period][bet_type].append(formatted_outcome)

        print(f"Formatted data for match {data['match_name']}: {formatted_data}")

        # Возвращаем отформатированные данные для отображения
        return {
            'match_name': f"{data['home_team']} vs {data['away_team']}",
            'time': datetime.datetime.fromtimestamp(data['time']).strftime("%Y-%m-%d %H:%M:%S"),
            'event_id': data['event_id'],
            'league': data['league'],
            'league_id': data['league_id'],
            'sport': data['sport'],
            'current_minute': data.get('current_minute', 'N/A'),
            'formatted_data': formatted_data
        }
    except Exception as e:
        # Логируем ошибку и возвращаем сообщение об ошибке
        print(f"Error in format_odds_data: {str(e)}")
        print(f"Input data: {data}")
        return {'error': f'Error processing data: {str(e)}'}


@app.route('/')
def home():
    """
    Обработчик главной страницы. Отображает список файлов с данными коэффициентов.

    :return: HTML страница со списком файлов
    """
    # Получаем список файлов в директории 'odds_data', оканчивающихся на '.bin'
    files = [f for f in os.listdir('odds_data') if f.endswith('.bin')]

    # HTML шаблон для отображения списка файлов
    html_template = """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Odds Data Home</title>
        <style>
            body { font-family: Arial, sans-serif; line-height: 1.6; padding: 20px; max-width: 800px; margin: 0 auto; }
            h1 { color: #333; }
            ul { list-style-type: none; padding: 0; }
            li { margin-bottom: 10px; }
            a { color: #0066cc; text-decoration: none; }
            a:hover { text-decoration: underline; }
        </style>
    </head>
    <body>
        <h1>Odds Data Files</h1>
        <ul>
        {% for file in files %}
            <li><a href="{{ url_for('get_odds', filename=file|urlencode) }}">{{ file }}</a></li>
        {% endfor %}
        </ul>
    </body>
    </html>
    """
    # Рендерим HTML страницу с переданным списком файлов
    return render_template_string(html_template, files=files)


@app.route('/get_odds')
def get_odds():
    """
    Обработчик страницы отображения данных конкретного матча.

    :return: HTML страница с информацией о матче и коэффициентами
    """
    filename = request.args.get('filename')
    if not filename:
        return "Filename not specified", 400  # Возвращаем ошибку, если имя файла не указано

    # HTML шаблон для отображения данных матча и коэффициентов
    html_template = """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Match Data</title>
        <style>
            body { font-family: Arial, sans-serif; line-height: 1.6; padding: 20px; max-width: 800px; margin: 0 auto; }
            h1 { color: #333; }
            h2 { color: #666; }
            .period { margin-bottom: 20px; }
            .bet-type { margin-bottom: 10px; }
            .outcomes { display: flex; flex-wrap: wrap; }
            .outcome { background-color: #f0f0f0; padding: 5px 10px; margin: 5px; border-radius: 5px; }
            .back-link { margin-top: 20px; }
            #error-message { color: red; }
        </style>
    </head>
    <body>
        <h1 id="match-name"></h1>
        <p id="event-info"></p>
        <div id="odds-data"></div>
        <p id="error-message"></p>
        <div class="back-link">
            <a href="{{ url_for('home') }}">Back to file list</a>
        </div>

        <script>
        function updateOdds() {
            fetch('/get_last_line?filename=' + encodeURIComponent('{{ filename }}'))
                .then(response => response.json())
                .then(data => {
                    console.log(data);
                    if (data.error) {
                        document.getElementById('error-message').textContent = data.error;
                        return;
                    }
                    document.getElementById('error-message').textContent = '';
                    document.getElementById('match-name').textContent = data.match_name;
                    document.getElementById('event-info').textContent = `Event ID: ${data.event_id || 'N/A'} | Time: ${data.time || 'N/A'} | League: ${data.league || 'N/A'} | League id: ${data.league_id} | Sport: ${data.sport || 'N/A'} | Current Minute: ${data.current_minute || 'N/A'}`;

                    let oddsHtml = '';
                    for (const [period, types] of Object.entries(data.formatted_data || {})) {
                        if (types && Object.keys(types).length > 0) {
                            oddsHtml += `<div class="period"><h2>${period}</h2>`;
                            for (const [betType, outcomes] of Object.entries(types || {})) {
                                oddsHtml += `<div class="bet-type"><h3>${betType}</h3><div class="outcomes">`;
                                for (const outcome of outcomes || []) {
                                    oddsHtml += `<span class="outcome">${outcome}</span>`;
                                }
                                oddsHtml += '</div></div>';
                            }
                            oddsHtml += '</div>';
                        }
                    }
                    document.getElementById('odds-data').innerHTML = oddsHtml;
                })
                .catch(error => {
                    console.error('Error:', error);
                    document.getElementById('error-message').textContent = 'Failed to fetch data. Please try again.';
                });
        }

        updateOdds();
        setInterval(updateOdds, 500);
        </script>
    </body>
    </html>
    """
    # Рендерим HTML страницу с передачей имени файла
    return render_template_string(html_template, filename=filename)


def read_bin_file(file_path):
    """
    Читает последний записанный объект из бинарного файла с использованием snappy.

    :param file_path: Путь к файлу
    :return: Данные из файла в виде словаря или None в случае ошибки
    """
    try:
        with open(file_path, 'rb') as f:
            length_data = f.read(4)  # Читаем первые 4 байта для получения длины сжатых данных
            if not length_data:
                return None

            # Распаковываем длину сжатых данных
            (compressed_length,) = struct.unpack('>I', length_data)
            # Читаем сжатые данные указанной длины
            compressed_data = f.read(compressed_length)
            # Декомпрессируем данные с помощью snappy
            decompressed_data = snappy.decompress(compressed_data)
            # Парсим JSON данные
            data = json.loads(decompressed_data.decode('utf-8'))
            return data
    except Exception as e:
        # Логируем ошибку и возвращаем None
        print(f"Error reading file: {str(e)}")
        return None


@app.route('/get_last_line')
def get_last_line():
    """
    Эндпоинт для получения последней записи из файла и ее форматирования.

    :return: JSON с отформатированными данными или сообщением об ошибке
    """
    filename = request.args.get('filename')
    if not filename:
        return jsonify({"error": "Filename not specified"}), 400  # Возвращаем ошибку, если имя файла не указано

    filename = unquote(filename)
    file_path = f"odds_data/{filename}"
    data = read_bin_file(file_path)

    if data is None:
        print("Invalid data structure in file")
        return jsonify({"error": "Invalid data structure in file"}), 400  # Возвращаем ошибку, если данные не прочитаны

    # Форматируем данные для отображения
    formatted_data = format_odds_data(data)

    if "error" in formatted_data:
        return jsonify(formatted_data), 400  # Возвращаем сообщение об ошибке из форматирования

    print("Formatted data successfully fetched:", formatted_data)  # Лог отформатированных данных
    return jsonify(formatted_data)  # Возвращаем данные в формате JSON


@app.template_filter('urlencode')
def urlencode_filter(s):
    """
    Пользовательский фильтр для кодирования строки в URL.

    :param s: Строка для кодирования
    :return: Закодированная строка
    """
    return quote(s)


if __name__ == '__main__':
    # Запускаем приложение Flask на порту 8000 в режиме отладки
    app.run(port=8000, debug=True)
