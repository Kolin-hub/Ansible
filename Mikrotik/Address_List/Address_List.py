import requests
import os
import re
import json
import yaml
import time
import gitlab
import fcntl
from lxml import html
from datetime import datetime

# Конфигурация
CONFLUENCE_API_URL = ''
CONFLUENCE_API_KEY = '-'
BASE_DIR = '/var/lib/awx/projects/_23__address_list/Address_List'
REMOVE_ADDRESS_YML_PATH = '/var/lib/awx/projects/_23__address_list/Address_List/Remove_Address.yml'

GITLAB_URL = ''
PRIVATE_TOKEN = ''
PROJECT_ID = 'admins/mikrotik'
GITLAB_FILE_PATH = 'Address_List/Remove_Address.yml'

gl = gitlab.Gitlab(GITLAB_URL, private_token=PRIVATE_TOKEN)
project = gl.projects.get(PROJECT_ID)

TELEGRAM_TOKEN = '-:-'
TELEGRAM_CHAT_ID = '-'
TELEGRAM_API_URL = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}"
LOG_FILE = '/var/lib/awx/projects/_23__address_list/Address_List/detailed_log.txt'
LOCK_FILE = '/var/lib/awx/projects/_23__address_list/Address_List/script.lock'

AWX_API_URL = ""
AWX_USERNAME = '-'
AWX_PASSWORD = '-'

ALLOWED_USERNAMES = [""]

pending_rules = {}
TELEGRAM_TIMEOUT = 300

def log_message(message):
    timestamp = datetime.now().strftime("[%Y-%m-%d %H:%M:%S]")
    log_entry = f"{timestamp} {message}"
    print(log_entry)
    with open(LOG_FILE, 'a') as f:
        f.write(log_entry + "\n")

def fetch_confluence_rules():
    headers = {'Authorization': f'Bearer {CONFLUENCE_API_KEY}'}
    try:
        response = requests.get(
            f"{CONFLUENCE_API_URL}/4268494?expand=body.storage,version",
            headers=headers,
            timeout=10
        )
        response.raise_for_status()
        
        page = response.json()
        if not page.get('id'):
            return []

        content = page['body']['storage']['value']
        tree = html.fromstring(content)
        
        rules = []
        for row in tree.xpath('//table//tr')[1:]:
            cells = [cell.text_content().strip() for cell in row.xpath('./td')]
            if len(cells) >= 4:
                rules.append(f"list={cells[2]} address={cells[1]}")
        return rules
        
    except Exception as e:
        log_message(f"Confluence fetch error: {str(e)}")
        return []

def normalize_mikrotik_rule(rule_text):
    params = dict(re.findall(r'(\S+)=("[^"]+"|\S+)', rule_text))
    return ' '.join([f'{k}={v}' for k, v in sorted(params.items())])

def process_mikrotik_file(file_path):
    try:
        with open(file_path, 'r') as f:
            content = f.read()
            
        rules = []
        current_flag = None
        current_rule_lines = []
        
        for line in content.splitlines():
            line = line.strip()
            
            # Пропускаем заголовки и пустые строки
            if not line or line.startswith("Flags:"):
                continue
                
            # Обработка новой записи с флагом
            if re.match(r'^\d+\s', line):
                # Сохраняем предыдущее правило
                if current_flag is not None and current_rule_lines:
                    rule_text = ' '.join(current_rule_lines)
                    rule_text = re.sub(r'\s+creation-time=.*', '', rule_text).strip()
                    if rule_text:  # Проверяем, что правило не пустое
                        rules.append((current_flag, normalize_mikrotik_rule(rule_text)))
                
                # Начинаем новое правило
                flag_match = re.match(r'^(\d+)', line)
                current_flag = int(flag_match.group(1))
                current_rule_lines = [re.sub(r'^\d+\s*', '', line).strip()]
            else:
                # Продолжение текущего правила
                current_rule_lines.append(line)
                
        # Обработка последнего правила в файле
        if current_flag is not None and current_rule_lines:
            rule_text = ' '.join(current_rule_lines)
            rule_text = re.sub(r'\s+creation-time=.*', '', rule_text).strip()
            if rule_text:
                rules.append((current_flag, normalize_mikrotik_rule(rule_text)))
                
        return rules
    except Exception as e:
        log_message(f"File processing error ({file_path}): {str(e)}")
        return []

def compare_rules(confluence_rules, mikrotik_rules):
    confluence_set = {normalize_mikrotik_rule(r) for r in confluence_rules}
    return [(flag, rule) for flag, rule in mikrotik_rules if rule not in confluence_set]

def send_telegram_message(message, reply_to=None):
    try:
        params = {
            'chat_id': TELEGRAM_CHAT_ID,
            'text': message
        }
        if reply_to:
            params['reply_to_message_id'] = reply_to
            
        response = requests.post(
            f"{TELEGRAM_API_URL}/sendMessage",
            json=params
        )
        return response.json()['result']['message_id']
    except Exception as e:
        log_message(f"Telegram send error: {str(e)}")
        return None

def get_telegram_updates(offset):
    try:
        response = requests.get(
            f"{TELEGRAM_API_URL}/getUpdates",
            params={'offset': offset, 'timeout': 10},
            timeout=15
        )
        return response.json().get('result', [])
    except Exception as e:
        log_message(f"Telegram update error: {str(e)}")
        return []

def create_remove_playbook(host, flag):
    playbook = [{
        'hosts': host,
        'tasks': [{
            'name': f'Remove address list {flag}',
            'community.routeros.command': {
                'commands': [f'/ip firewall address-list remove numbers={flag}']
            }
        }]
    }]
    
    try:
        with open(REMOVE_ADDRESS_YML_PATH, 'w') as f:
            yaml.safe_dump(playbook, f, default_flow_style=False)
            
        upload_to_gitlab()
        return True
    except Exception as e:
        log_message(f"Playbook creation error: {str(e)}")
        return False

def upload_to_gitlab():
    try:
        with open(REMOVE_ADDRESS_YML_PATH, 'r') as f:
            content = f.read()
        
        try:
            file = project.files.get(file_path=GITLAB_FILE_PATH, ref='master')
            file.delete(branch='master', commit_message='Remove old playbook')
        except gitlab.exceptions.GitlabGetError:
            pass
        except Exception as e:
            log_message(f"GitLab delete error: {str(e)}")
            return False
            
        project.files.create({
            'file_path': GITLAB_FILE_PATH,
            'branch': 'master',
            'content': content,
            'commit_message': 'Update remove playbook'
        })
        return True
    except Exception as e:
        log_message(f"GitLab error: {str(e)}")
        return False

def launch_awx_job():
    try:
        response = requests.post(
            AWX_API_URL,
            auth=(AWX_USERNAME, AWX_PASSWORD),
            timeout=30
        )
        return response.json().get('id')
    except Exception as e:
        log_message(f"AWX launch error: {str(e)}")
        return None

def add_rule_to_confluence(rule_text, mikrotik_name):
    headers = {
        'Authorization': f'Bearer {CONFLUENCE_API_KEY}',
        'Content-Type': 'application/json'
    }
    
    try:
        list_match = re.search(r'list=([\w_]+)', rule_text)
        address_match = re.search(r'address=([\d./]+)', rule_text)
        if not list_match or not address_match:
            raise ValueError("Invalid rule format for adding to Confluence.")

        list_name = list_match.group(1)
        ip_address = address_match.group(1)

        response = requests.get(
            f"{CONFLUENCE_API_URL}/4268494?expand=body.storage,version",
            headers=headers
        )
        response.raise_for_status()

        page = response.json()
        if 'id' not in page:
             raise ValueError("Page not found")
             
        page_id = page['id']
        current_version = page['version']['number']
        current_content = page['body']['storage']['value']
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
            <td>{ip_address}</td>
            <td>{list_name}</td>
            <td>{mikrotik_name}</td>
        </tr>
        """

        table_end_index = current_content.rfind('</tr>') + 5
        new_content = current_content[:table_end_index] + new_row + current_content[table_end_index:]

        update_payload = {
            "id": page_id,
            "type": "page",
            "title": "Address List",
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

        response = requests.put(
            f"{CONFLUENCE_API_URL}/{page_id}",
            headers=headers, 
            data=json.dumps(update_payload)
        )
        response.raise_for_status()
        
        log_message(f"Правило успешно добавлено в Confluence: {rule_text}")
        
    except Exception as e:
        log_message(f"Ошибка добавления в Confluence: {str(e)}")
        raise

def process_message(message):
    global pending_rules
    
    msg = message.get('message', {})
    reply_to = msg.get('reply_to_message', {}).get('message_id')
    username = msg.get('from', {}).get('username')
    
    if not reply_to or reply_to not in pending_rules:
        return
    
    if username not in ALLOWED_USERNAMES:
        send_telegram_message("Доступ запрещен!", reply_to=msg['message_id'])
        return
    
    mikrotik_name, flag, rule = pending_rules.pop(reply_to)
    command = msg.get('text', '').lower()
    
    if command == 'удалить':
        handle_remove_action(mikrotik_name, flag, msg['message_id'])
    elif command == 'добавить':
        handle_add_action(rule, mikrotik_name, msg['message_id'])
    else:
        send_telegram_message("Некорректная команда. Используйте 'Добавить' или 'Удалить'", reply_to=msg['message_id'])

def handle_remove_action(host, flag, message_id):
    try:
        if not create_remove_playbook(host, flag):
            raise Exception("Ошибка создания плейбука")
            
        job_id = launch_awx_job()
        if not job_id:
            raise Exception("Ошибка запуска AWX")
            
        start_time = time.time()
        while time.time() - start_time < 300:
            try:
                response = requests.get(
                    f"http://******:8880/api/v2/jobs/{job_id}/",
                    auth=(AWX_USERNAME, AWX_PASSWORD),
                    timeout=10
                )
                status = response.json().get('status')
                
                if status == 'successful':
                    send_telegram_message(f"Правило {flag} успешно удалено!", reply_to=message_id)
                    return
                elif status == 'failed':
                    raise Exception("AWX job failed")
                    
                time.sleep(5)
                
            except Exception as e:
                log_message(f"AWX status check error: {str(e)}")
                continue
                
        raise Exception("Timeout waiting for AWX")
        
    except Exception as e:
        send_telegram_message(f"Ошибка удаления: {str(e)}", reply_to=message_id)
        log_message(f"Remove action error: {str(e)}")

def handle_add_action(rule, mikrotik_name, message_id):
    try:
        add_rule_to_confluence(rule, mikrotik_name)
        send_telegram_message("Правило добавлено в Confluence!", reply_to=message_id)
    except Exception as e:
        send_telegram_message(f"Ошибка добавления: {str(e)}", reply_to=message_id)
        log_message(f"Add action error: {str(e)}")

def main():
    with open(LOCK_FILE, 'w') as lock_file:
        try:
            fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            log_message("Скрипт уже выполняется")
            return

        try:
            log_message("Скрипт запущен")
            
            confluence_rules = fetch_confluence_rules()
            log_message(f"Загружено {len(confluence_rules)} правил из Confluence")
            
            for filename in os.listdir(BASE_DIR):
                if filename.endswith('_Address_List.txt'):
                    mikrotik_name = filename.replace('_Address_List.txt', '')
                    file_path = os.path.join(BASE_DIR, filename)
                    
                    mikrotik_rules = process_mikrotik_file(file_path)
                    log_message(f"Обработка {mikrotik_name}: найдено {len(mikrotik_rules)} правил")
                    
                    differences = compare_rules(confluence_rules, mikrotik_rules)
                    
                    if differences:
                        log_message(f"Найдено {len(differences)} расхождений для {mikrotik_name}")
                        
                        for flag, rule in differences:
                            # Извлекаем параметры правила
                            params = dict(re.findall(r'(\S+)=("[^"]+"|\S+)', rule))
                            
                            # Форматируем для отображения
                            list_value = params.get('list', '')
                            address_value = params.get('address', '')
                            
                            # Если параметры не найдены, показываем сырое правило
                            if not list_value or not address_value:
                                formatted_rule = rule
                            else:
                                formatted_rule = f"list={list_value} address={address_value}"
                            
                            message_text = (
                                f'Обнаружено различие в Address List на "{mikrotik_name}":\n'
                                f'{formatted_rule} Flag {flag}\n'
                                'Ответьте "Добавить", чтобы добавить в Confluence, или "Удалить", чтобы удалить Address с MikroTik.'
                            )
                            msg_id = send_telegram_message(message_text)
                            if msg_id:
                                pending_rules[msg_id] = (mikrotik_name, flag, rule)
                                log_message(f"Отправлено уведомление: {rule}")
            
            offset = 0
            start_time = time.time()
            while pending_rules and (time.time() - start_time) < TELEGRAM_TIMEOUT:
                updates = get_telegram_updates(offset)
                for update in updates:
                    offset = update['update_id'] + 1
                    process_message(update)
                time.sleep(1)
            
            log_message("Скрипт завершен")
            
        except Exception as e:
            log_message(f"Критическая ошибка: {str(e)}")
        finally:
            fcntl.flock(lock_file, fcntl.LOCK_UN)

if __name__ == "__main__":
    main()