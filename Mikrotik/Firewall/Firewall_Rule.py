import requests
import os
import re
import json
import yaml
import time
import gitlab
from lxml import html

# Конфигурация
CONFLUENCE_URL = ''
CONFLUENCE_API_URL = ''
CONFLUENCE_API_KEY = '-'
BASE_DIR = '/var/lib/awx/projects/_15__firewall/Firewall'
REMOVE_FIREWALL_YML_PATH = '/var/lib/awx/projects/_15__firewall/Firewall/Remove_firewall.yml'

GITLAB_URL = ''
PRIVATE_TOKEN = '-'
PROJECT_ID = ''
GITLAB_FILE_PATH = ''

gl = gitlab.Gitlab(GITLAB_URL, private_token=PRIVATE_TOKEN)
project = gl.projects.get(PROJECT_ID)

TELEGRAM_TOKEN = '-:-'
TELEGRAM_CHAT_ID = '-'
TELEGRAM_API_URL = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}"

AWX_API_URL = ""
AWX_USERNAME = '-'
AWX_PASSWORD = '-'

ALLOWED_USERNAMES = [""]  
pending_rules = {}
TELEGRAM_TIMEOUT = 300

def fetch_confluence_rules(url, api_key):
    headers = {'Authorization': f'Bearer {api_key}', 'Content-Type': 'application/json'}
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"Error fetching Confluence rules: {e}")
        return []

    tree = html.fromstring(response.content)
    rules = [
        rule_tag.text_content().strip()
        for rule_tag in tree.xpath('//td[contains(@class, "confluenceTd")]')
        if rule_tag.text_content().strip().startswith('add action=')
    ]
    return rules

def normalize_mikrotik_rules(file_content):
    lines = file_content.splitlines()
    rules = []
    current_rule = {}

    for line in lines:
        line = line.strip()

        if re.match(r"^\d+\s*;;;.*", line):
            if current_rule:
                rules.append(current_rule)
            index, comment = line.split(";;;")
            current_rule = {'index': index.strip(), 'comment': comment.strip(), 'params': []}
        elif re.match(r"^\d+\s+.*", line):
            if current_rule:
                rules.append(current_rule)
            parts = line.split(None, 1)
            current_rule = {'index': parts[0].strip(), 'comment': None, 'params': [parts[1].strip()]}
        elif current_rule:
            current_rule['params'].append(line.strip())

    if current_rule:
        rules.append(current_rule)

    formatted_rules = []
    for rule in rules:
        params = ' '.join(rule['params'])
        params = re.sub(r'log=no', '', params)
        params = re.sub(r'log-prefix="[^"]*"', '', params)
        params = re.sub(r'log-prefix=[^ ]+', '', params)
        params = re.sub(r'\s+', ' ', params).strip()

        param_dict = parse_params(params)
        sorted_params = ' '.join(f'{k}={v}' for k, v in sorted(param_dict.items()))

        if rule['comment']:
            formatted_rules.append(f'add {sorted_params} comment="{rule["comment"]}" Flags: {rule["index"]}')
        else:
            formatted_rules.append(f'add {sorted_params} Flags: {rule["index"]}')

    return formatted_rules

def parse_params(param_string):
    param_pairs = re.findall(r'(\S+)=("[^"]*"|\S+)', param_string)
    param_dict = {}
    for k, v in param_pairs:
        value = v.strip('"')
        value = ' '.join(value.split())
        param_dict[k] = value
    return param_dict

import re

def normalize_rule(rule_text):
    # Убираем метки Flags
    rule_text = re.sub(r'Flags:\s*\d+', '', rule_text).strip()

    # Извлекаем параметры после 'add'
    param_match = re.match(r'add (.*)', rule_text)
    if not param_match:
        return ''

    # Извлекаем параметры в строку
    params = param_match.group(1)

    # Убираем параметры логирования
    params = re.sub(r'log=no', '', params)
    params = re.sub(r'log-prefix="[^"]*"', '', params)
    params = re.sub(r'log-prefix=[^ ]+', '', params)
    params = re.sub(r'\s+', ' ', params).strip()

    # Преобразуем параметры в словарь
    param_dict = parse_params(params)
    param_dict = {k: v.strip('"').strip() for k, v in param_dict.items()}

    # Убедимся, что параметры будут в правильном порядке
    ordered_params = ['action', 'chain', 'comment', 'dst-address', 'protocol']
    sorted_params = {k: param_dict[k] for k in ordered_params if k in param_dict}

    # Формируем нормализованное правило
    normalized_rule = 'add ' + ' '.join(f'{k}={v}' for k, v in sorted_params.items())

    # Возвращаем нормализованное правило
    return normalized_rule

def compare_rules(confluence_rules, mikrotik_rules):
    differences = []
    mikrotik_rules_set = {normalize_rule(rule) for rule in mikrotik_rules}
    confluence_rules_set = {normalize_rule(rule) for rule in confluence_rules}

    for rule in mikrotik_rules:
        normalized_rule = normalize_rule(rule)
        if normalized_rule not in confluence_rules_set:
            differences.append(rule)
    return differences

def send_telegram_message(chat_id, message, reply_to_message_id=None):
    url = f"{TELEGRAM_API_URL}/sendMessage"
    payload = {'chat_id': chat_id, 'text': message, 'reply_to_message_id': reply_to_message_id}
    try:
        response = requests.post(url, data=payload)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"Error sending Telegram message: {e}")
        return None

def get_telegram_updates(offset=None):
    url = f"{TELEGRAM_API_URL}/getUpdates"
    payload = {'offset': offset} if offset else {}
    try:
        response = requests.get(url, params=payload)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"Error getting Telegram updates: {e}")
        return None

def add_rule_to_confluence(rule_text, comment, mikrotik_name):
    headers = {'Authorization': f'Bearer {CONFLUENCE_API_KEY}', 'Content-Type': 'application/json'}

    response = requests.get(
        f"{CONFLUENCE_API_URL}?title=Firewall_NEW&spaceKey=AdmITS&expand=body.storage,version",
        headers=headers
    )
    response.raise_for_status()
    page_info = response.json()

    if not page_info['results']:
        raise ValueError("Страница 'Firewall_NEW' не найдена в пространстве 'AdmITS'. Проверьте название страницы и ключ пространства.")

    page_id = page_info['results'][0]['id']
    current_version = page_info['results'][0]['version']['number']
    current_content = page_info['results'][0]['body']['storage']['value']
    tree = html.fromstring(current_content)
    rows = tree.xpath('//tr')

    last_number = 0
    for row in rows:
        try:
            number = int(row.xpath('./td[1]/text()')[0].strip())
            last_number = max(last_number, number)
        except (IndexError, ValueError):
            continue

    new_number = last_number + 1
    new_row = f"""
    <tr>
        <td>{new_number}</td>
        <td>{comment}</td>
        <td>{rule_text}</td>
        <td></td>
        <td>{mikrotik_name}</td>
    </tr>
    """

    table_end_index = current_content.rfind('</tr>') + 5
    new_content = current_content[:table_end_index] + new_row + current_content[table_end_index:]

    update_payload = {
        "id": page_id,
        "type": "page",
        "title": "Firewall_NEW",
        "body": {
            "storage": {
                "value": new_content,
                "representation": "storage"
            }
        },
        "version": {
            "number": current_version + 1
        }
    }

    response = requests.put(f"{CONFLUENCE_API_URL}/{page_id}", headers=headers, data=json.dumps(update_payload))
    response.raise_for_status()

def create_remove_playbook_with_flags(host, flags_number):
    playbook_data = [{
        'hosts': host,
        'ignore_unreachable': True,
        'gather_facts': False,
        'tasks': [{
            'name': f'Удаление правила с номером {flags_number}',
            'community.routeros.command': {
                'commands': [f'/ip firewall filter remove numbers={flags_number}']
            }
        }]
    }]

    with open(REMOVE_FIREWALL_YML_PATH, 'w') as yml_file:
        yaml.dump(playbook_data, yml_file, default_flow_style=False, allow_unicode=True)

def upload_remove_firewall_to_gitlab():
    if os.path.exists(REMOVE_FIREWALL_YML_PATH):
        with open(REMOVE_FIREWALL_YML_PATH, 'r', encoding='utf-8') as file:
            file_content = file.read()
            try:
                project.files.get(file_path=GITLAB_FILE_PATH, ref='master').delete(branch='master', commit_message=f'Delete {GITLAB_FILE_PATH}')
            except gitlab.exceptions.GitlabGetError:
                pass
            project.files.create({
                'file_path': GITLAB_FILE_PATH,
                'branch': 'master',
                'content': file_content,
                'commit_message': f'Add {GITLAB_FILE_PATH}'
            })

def launch_awx_remove_playbook():
    response = requests.post(AWX_API_URL, auth=(AWX_USERNAME, AWX_PASSWORD))
    response.raise_for_status()
    return response.json()

def get_awx_job_result(job_id):
    job_url = f"http://*****:8880/api/v2/jobs/{job_id}/"
    response = requests.get(job_url, auth=(AWX_USERNAME, AWX_PASSWORD))
    response.raise_for_status()
    return response.json().get('status')

def process_message(update):
    chat_id = update['message']['chat']['id']
    text = update['message'].get('text')
    reply_to_message_id = update['message'].get('reply_to_message', {}).get('message_id')
    username = update['message']['from'].get('username')

    if username not in ALLOWED_USERNAMES:
        send_telegram_message(chat_id, "Вы не авторизованы для выполнения этой команды.", reply_to_message_id)
        return

    if reply_to_message_id and reply_to_message_id in pending_rules:
        mikrotik_name, rule = pending_rules[reply_to_message_id].split(': ', 1)
        flags_match = re.search(r'Flags: (\d+)', rule)
        if not flags_match:
            send_telegram_message(chat_id, "Невозможно найти Flags в правиле.", reply_to_message_id)
            del pending_rules[reply_to_message_id]  # Удаляем запись
            return

        flags_number = flags_match.group(1)

        if text == 'Удалить':
            try:
                create_remove_playbook_with_flags(mikrotik_name, flags_number)
                upload_remove_firewall_to_gitlab()
                job_response = launch_awx_remove_playbook()
                job_id = job_response.get('id')
                while True:
                    status = get_awx_job_result(job_id)
                    if status in ['successful', 'failed']:
                        break
                    time.sleep(5)
                if status == 'successful':
                    send_telegram_message(chat_id, "Правило успешно удалено.", reply_to_message_id)
                else:
                    send_telegram_message(chat_id, "Правило не удалено.", reply_to_message_id)
            except Exception as e:
                send_telegram_message(chat_id, f"Ошибка при удалении: {e}", reply_to_message_id)
            finally:
                del pending_rules[reply_to_message_id]

        elif text == 'Добавить':
            try:
                rule_without_flags = re.sub(r'Flags: \d+', '', rule).strip()
                comment_match = re.search(r'comment="([^"]+)"', rule)
                comment = comment_match.group(1) if comment_match else ''
                add_rule_to_confluence(rule_without_flags, comment, mikrotik_name)
                send_telegram_message(chat_id, "Правило успешно добавлено в Confluence.", reply_to_message_id)
            except Exception as e:
                send_telegram_message(chat_id, f"Ошибка при добавлении правила: {e}", reply_to_message_id)
            finally:
                del pending_rules[reply_to_message_id]

def main():
    confluence_rules = fetch_confluence_rules(CONFLUENCE_URL, CONFLUENCE_API_KEY)

    mikrotik_differences = {}

    for file_name in os.listdir(BASE_DIR):
        if file_name.endswith('_firewall_rules.txt'):
            mikrotik_name = file_name.replace('_firewall_rules.txt', '')
            file_path = os.path.join(BASE_DIR, file_name)

            if not os.path.exists(file_path):
                print(f"Warning: file {file_path} not found, skipping processing.")
                continue

            with open(file_path, 'r') as f:
                mikrotik_rules_content = f.read()
            mikrotik_rules = normalize_mikrotik_rules(mikrotik_rules_content)
            differences = compare_rules(confluence_rules, mikrotik_rules)

            if differences:
                mikrotik_differences[mikrotik_name] = differences

            os.remove(file_path)

    for mikrotik_name, differences in mikrotik_differences.items():
        for diff in differences:
            message_text = (
                f"Обнаружено различие в правилах на {mikrotik_name}:\n\n{diff}\n\n"
                "Пожалуйста, ответьте 'Добавить', чтобы добавить в Confluence, или 'Удалить', чтобы удалить правило с MikroTik."
            )
            response = send_telegram_message(TELEGRAM_CHAT_ID, message_text)
            if response:
                pending_rules[response['result']['message_id']] = f"{mikrotik_name}: {diff}"

    last_update_id = None
    while pending_rules:
        updates = get_telegram_updates(last_update_id)
        if updates:
            for update in updates['result']:
                process_message(update)
                last_update_id = update['update_id'] + 1
            time.sleep(5)

if __name__ == "__main__":
    main()