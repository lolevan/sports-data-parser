import datetime

from flask import Flask, request, render_template_string, jsonify
from collections import defaultdict
import json
import os
from urllib.parse import quote, unquote

app = Flask(__name__)

def format_odds_data(data):
    try:
        formatted_data = {
            'Match': defaultdict(list),
            '1H': defaultdict(list),
            '2H': defaultdict(list)
        }

        for outcome in data['outcomes']:
            period = 'Match'
            if outcome['type_name'].startswith('1H'):
                period = '1H'
            elif outcome['type_name'].startswith('2H'):
                period = '2H'

            bet_type = outcome['type_name'].replace('1H', '').replace('2H', '')
            formatted_outcome = f"{outcome['type']}: {outcome.get('line', 'N/A')} @ {outcome['odds']}"
            formatted_data[period][bet_type].append(formatted_outcome)

        return {
            'match_name': f"{data['home_team']} vs {data['away_team']}",
            'time': datetime.datetime.fromtimestamp(data['time']).strftime("%Y-%m-%d %H:%M:%S"),
            'event_id': data['event_id'],
            'league': data['league'],
            'sport': data['sport'],
            'current_minute': data.get('current_minute', 0),
            'formatted_data': formatted_data
        }
    except Exception as e:
        print(f"Error in format_odds_data: {str(e)}")
        print(f"Input data: {data}")
        return {'error': f'Error processing data: {str(e)}'}

@app.route('/')
def home():
    files = [f for f in os.listdir('odds_data') if f.endswith('.jsonl')]
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
    return render_template_string(html_template, files=files)

@app.route('/get_odds')
def get_odds():
    filename = request.args.get('filename')
    if not filename:
        return "Filename not specified", 400

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
                    if (data.error) {
                        document.getElementById('error-message').textContent = data.error;
                        return;
                    }
                    document.getElementById('error-message').textContent = '';
                    document.getElementById('match-name').textContent = data.match_name;
                    document.getElementById('event-info').textContent = `Event ID: ${data.event_id} | Time: ${data.time} | League: ${data.league} | Sport: ${data.sport} | Current Minute: ${data.current_minute}`;

                    let oddsHtml = '';
                    for (const [period, types] of Object.entries(data.formatted_data)) {
                        if (Object.keys(types).length > 0) {
                            oddsHtml += `<div class="period"><h2>${period}</h2>`;
                            for (const [betType, outcomes] of Object.entries(types)) {
                                oddsHtml += `<div class="bet-type"><h3>${betType}</h3><div class="outcomes">`;
                                for (const outcome of outcomes) {
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
    return render_template_string(html_template, filename=filename)

@app.route('/get_last_line')
def get_last_line():
    filename = request.args.get('filename')
    if not filename:
        return jsonify({"error": "Filename not specified"}), 400

    filename = unquote(filename)

    try:
        with open(f"odds_data/{filename}", 'r') as file:
            lines = file.readlines()
            if not lines:
                return jsonify({"error": "File is empty"}), 404

            # Get the last line and parse it as JSON
            last_line = lines[-1].strip()
            data = json.loads(last_line)
            return jsonify(format_odds_data(data))

    except FileNotFoundError:
        return jsonify({"error": f"File not found: {filename}"}), 404
    except json.JSONDecodeError:
        return jsonify({"error": "Invalid JSON in the last line"}), 400
    except Exception as e:
        print(f"Unexpected error in get_last_line: {str(e)}")
        return jsonify({"error": f"Unexpected error: {str(e)}"}), 500

@app.template_filter('urlencode')
def urlencode_filter(s):
    return quote(s)

if __name__ == '__main__':
    app.run(port=8002, debug=True)