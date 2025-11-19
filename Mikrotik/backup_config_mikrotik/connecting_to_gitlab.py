import os
import hashlib
import gitlab
import requests
import difflib
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Переменные
GITLAB_URL = ''
PRIVATE_TOKEN = '-'  
PROJECT_ID = 'admins/mikrotik'  
CONFIG_DIR = '/var/lib/awx/projects/_12__backup_network_devices/backup_config_mikrotik/config/'

# Переменные для Telegram
TELEGRAM_TOKEN = ''  
TELEGRAM_CHAT_ID = ''  

# Авторизация в GitLab
gl = gitlab.Gitlab(GITLAB_URL, private_token=PRIVATE_TOKEN)
project = gl.projects.get(PROJECT_ID)

def clean_content(content):
    """Remove leading slash and initial empty lines, keep all other content."""
    lines = content.splitlines()
    cleaned_lines = []

    if lines and lines[0].startswith('/'):
        lines[0] = lines[0][1:]

    while lines and lines[0].strip() == "":
        lines.pop(0)

    cleaned_lines.extend(lines)
    return '\n'.join(cleaned_lines)

def clean_content_for_checksum(content):
    """Prepare content for checksum calculation by removing lines starting with # and leading slash and empty lines."""
    lines = content.splitlines()
    cleaned_lines = []

    if lines and lines[0].startswith('/'):
        lines[0] = lines[0][1:]

    while lines and lines[0].strip() == "":
        lines.pop(0)

    for line in lines:
        if not line.startswith('#'):
            cleaned_lines.append(line)

    return '\n'.join(cleaned_lines)

def get_file_checksum(content):
    """Calculate the MD5 checksum of file content after cleaning for checksum."""
    cleaned_content = clean_content_for_checksum(content)
    md5 = hashlib.md5()
    md5.update(cleaned_content.encode('utf-8'))
    return md5.hexdigest()

def get_gitlab_file_content(project, file_path, branch='master'):
    """Retrieve file content from GitLab."""
    try:
        file = project.files.get(file_path=file_path, ref=branch)
        return file.decode().decode('utf-8')
    except gitlab.exceptions.GitlabGetError:
        return None

def upload_to_gitlab(project, file_path, file_content, action):
    """Upload file to GitLab."""
    gitlab_action = "create" if action == "созда" else "update"
    
    data = {
        'branch': 'master',
        'commit_message': f'{"Update" if gitlab_action == "update" else "Add"} {file_path}',
        'actions': [
            {
                'action': gitlab_action,
                'file_path': file_path,
                'content': file_content
            }
        ]
    }
    try:
        project.commits.create(data)
        logging.info(f'Файл {file_path} успешно {action}н.')
        return True
    except Exception as e:
        logging.error(f'Не удалось {action} файл {file_path} в GitLab: {e}')
        return False

def get_diff(old_content, new_content):
    """Get a filtered diff with only added or removed lines, removing unnecessary info."""
    old_lines = old_content.splitlines()
    new_lines = new_content.splitlines()
    diff = difflib.unified_diff(old_lines, new_lines, lineterm='', n=0)
    
    filtered_diff = [
        line for line in diff
        if not line.startswith('@@')  # Убираем строки, начинающиеся с @@
    ]
    
    return '\n'.join(filtered_diff)

def filter_message(message):
    """Filter out the first two lines, lines starting with @@, and lines starting with -# and +#."""
    lines = message.splitlines()
    filtered_lines = []

    # Убираем первые две строки
    if len(lines) > 2:
        lines = lines[2:]

    # Убираем строки, начинающиеся с @@, -# и +#
    filtered_lines = [line for line in lines 
                      if not (line.startswith('@@') or line.startswith('-#') or line.startswith('+#'))]

    return '\n'.join(filtered_lines)

def extract_section_from_diff(content, diff):
    """Extract the section name from the content based on the diff."""
    lines = content.splitlines()
    diff_lines = diff.splitlines()
    
    # Найдем все разделы
    sections = []
    for i, line in enumerate(lines):
        if line.startswith('/'):
            sections.append((i, line.strip()))

    # Поиск изменений и ближайшего раздела сверху
    last_section = 'unknown'
    for diff_line in diff_lines:
        if diff_line.startswith('+') or diff_line.startswith('-'):
            for i, line in enumerate(lines):
                if line.strip() == diff_line[1:].strip():
                    # Найти ближайший раздел сверху
                    for j in range(len(sections) - 1, -1, -1):
                        if sections[j][0] <= i:
                            return sections[j][1]
                    return last_section

    return last_section

def send_telegram_message(message):
    """Send a message to Telegram."""
    url = f'https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage'
    data = {'chat_id': TELEGRAM_CHAT_ID, 'text': message}
    try:
        response = requests.post(url, data=data)
        response.raise_for_status()
        logging.info(f'Успешно отправлено сообщение в Telegram: {message}')
    except requests.exceptions.RequestException as e:
        logging.error(f'Не удалось отправить сообщение в Telegram: {e}')

def main():
    changes = {}
    for file_name in os.listdir(CONFIG_DIR):
        file_path = os.path.join(CONFIG_DIR, file_name)
        if os.path.isfile(file_path):
            with open(file_path, 'r', encoding='utf-8') as f:
                raw_config_content = f.read()

            cleaned_config_content = clean_content(raw_config_content)
            gitlab_file_path = f'backup_config_mikrotik/config/{file_name}'
            current_config_content = get_gitlab_file_content(project, gitlab_file_path)

            if current_config_content is None:
                if upload_to_gitlab(project, gitlab_file_path, cleaned_config_content, 'созда'):
                    changes[file_name] = 'Файл был создан в GitLab.'
            else:
                current_checksum = get_file_checksum(current_config_content)
                new_checksum = get_file_checksum(cleaned_config_content)

                if current_checksum != new_checksum:
                    diff = get_diff(current_config_content, cleaned_config_content)
                    filtered_diff = filter_message(diff)
                    section = extract_section_from_diff(cleaned_config_content, filtered_diff)
                    if upload_to_gitlab(project, gitlab_file_path, cleaned_config_content, 'обновле'):
                        # Ссылка на GitLab вместо детализированного списка изменений
                        gitlab_link = f'{GITLAB_URL}/{PROJECT_ID}/-/tree/master/backup_config_mikrotik/config'
                        changes[file_name] = f'Файл был обновлен в GitLab. Изменения:\n{gitlab_link}'

    # Отправка сообщений в Telegram по каждому измененному файлу
    for file_name, message in changes.items():
        send_telegram_message(f'Файл: {file_name}\n{message}')

if __name__ == "__main__":
    main()