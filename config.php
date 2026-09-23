<?php
/**
 * Конфигурационный файл для API Hollyhop
 * 
 * Конфигурация загружается из файла .env
 * Копируйте .env.example в .env и отредактируйте под ваши нужды:
 * 
 * cp .env.example .env
 * 
 * Затем отредактируйте .env:
 * HOLLYHOP_SUBDOMAIN=your_subdomain
 * HOLLYHOP_AUTH_KEY=your_auth_key_here
 */

/**
 * Загрузить файл .env
 */
function load_env_file($file_path = null) {
    if ($file_path === null) {
        $file_path = __DIR__ . '/.env';
    }
    
    if (!file_exists($file_path)) {
        throw new Exception("Файл конфигурации не найден: {$file_path}\n" .
            "Скопируйте .env.example в .env:\n" .
            "  cp .env.example .env");
    }
    
    $lines = file($file_path, FILE_IGNORE_NEW_LINES | FILE_SKIP_EMPTY_LINES);
    
    foreach ($lines as $line) {
        // Пропускаем комментарии и пустые строки
        if (strpos(trim($line), '#') === 0 || empty(trim($line))) {
            continue;
        }
        
        // Парсим переменные
        if (strpos($line, '=') !== false) {
            list($key, $value) = explode('=', $line, 2);
            $key = trim($key);
            $value = trim($value);
            
            // Удаляем кавычки если есть
            if ((substr($value, 0, 1) === '"' && substr($value, -1) === '"') ||
                (substr($value, 0, 1) === "'" && substr($value, -1) === "'")) {
                $value = substr($value, 1, -1);
            }
            
            // Устанавливаем переменную окружения
            putenv("{$key}={$value}");
            $_ENV[$key] = $value;
        }
    }
}

/**
 * Загружаем конфигурацию из .env файла
 */
try {
    load_env_file();
} catch (Exception $e) {
    error_log("Ошибка конфигурации: " . $e->getMessage());
    // Продолжаем со значениями по умолчанию (для обратной совместимости)
}

// Получаем параметры конфигурации
$config = [
    'amocrm' => [
        'client_id' => getenv('AMOCRM_CLIENT_ID') ?: 'e2776d4925914663886c992d8e29fc5b',
        'client_secret' => getenv('AMOCRM_CLIENT_SECRET') ?: '6abacd33ad3143f7a8cde57e2b849c1a',
        'redirect_uri' => getenv('AMOCRM_REDIRECT_URI') ?: 'https://srm.chinatutor.ru/hook.php',
        'subdomain' => getenv('AMOCRM_SUBDOMAIN') ?: 'directorchinatutorru',
    ],
    'api' => [
        'subdomain' => getenv('HOLLYHOP_SUBDOMAIN') ?: 'your_subdomain',
        'auth_key' => getenv('HOLLYHOP_AUTH_KEY') ?: 'your_auth_key',
        'base_url' => 'https://' . (getenv('HOLLYHOP_SUBDOMAIN') ?: 'your_subdomain') . '.t8s.ru/Api/V2'
    ],
    'yandex_forms' => [
        'api_base_url' => getenv('YANDEX_FORMS_API_BASE_URL') ?: 'https://api.forms.yandex.net/v1',
        'token' => getenv('YANDEX_FORMS_TOKEN') ?: 'y0__wgBEILGv6Sq94ACGNzcRiDIoqTGGFfZfp2LqpV7YFtMbeBh3m0kVWzT',
        'org_id' => getenv('YANDEX_FORMS_ORG_ID') ?: '',
        'lead_param_name' => getenv('YANDEX_FORMS_LEAD_PARAM_NAME') ?: 'amo_lead_id',
        'portal_param_name' => getenv('YANDEX_FORMS_PORTAL_PARAM_NAME') ?: 'amo_portal',
        'public_base_url' => rtrim(getenv('YANDEX_FORMS_PUBLIC_BASE_URL') ?: 'https://srm.chinatutor.ru', '/'),
        'results_field_id' => 1639409,
        'secondary_results_field_id' => 1902565,
        'task_enabled' => true,
        'task_text' => 'Проверить ответ Яндекс.Формы',
        'task_due_hours' => 24,
        'widget_server_url' => rtrim(getenv('YANDEX_FORMS_WIDGET_SERVER_URL') ?: 'https://srm.chinatutor.ru/yandex_forms_api.php', '/'),
        'storage_dir' => __DIR__ . '/data/yandex_forms',
    ],
    'amocrm_portals' => [
        'directorchinatutorru' => [
            'client_id' => getenv('AMOCRM_CLIENT_ID') ?: 'e2776d4925914663886c992d8e29fc5b',
            'client_secret' => getenv('AMOCRM_CLIENT_SECRET') ?: '6abacd33ad3143f7a8cde57e2b849c1a',
            'redirect_uri' => getenv('AMOCRM_REDIRECT_URI') ?: 'https://srm.chinatutor.ru/hook.php',
            'subdomain' => getenv('AMOCRM_SUBDOMAIN') ?: 'directorchinatutorru',
            'tokens_file' => getenv('AMOCRM_TOKENS_FILE') ?: (__DIR__ . '/tokens.json'),
            'results_field_id' => 1639409,
            'results_history_field_id' => 1640135,
        ],
        'secondary' => [
            'client_id' => getenv('AMOCRM_SECONDARY_CLIENT_ID') ?: '',
            'client_secret' => getenv('AMOCRM_SECONDARY_CLIENT_SECRET') ?: '',
            'redirect_uri' => getenv('AMOCRM_SECONDARY_REDIRECT_URI') ?: '',
            'subdomain' => getenv('AMOCRM_SECONDARY_SUBDOMAIN') ?: 'supportchinatutorru',
            'tokens_file' => getenv('AMOCRM_SECONDARY_TOKENS_FILE') ?: (__DIR__ . '/tokens_secondary.json'),
            'access_token' => getenv('AMOCRM_SECONDARY_ACCESS_TOKEN') ?: 'eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsImp0aSI6ImM2MDc4MDY1ZTczN2IwOWU1MTcxMDY5ZjUzZmIyN2QwNzBjMTA3YzIyODI0NzAxNzAzNjdhZGI2NzlhM2MwOWEyNzM1NGI5ZDk5ZjE1ZDMzIn0.eyJhdWQiOiIwNTQ0ZDY3NC00MmI4LTRiYTMtOTBlZC02OTM4MGFkMzNjNWQiLCJqdGkiOiJjNjA3ODA2NWU3MzdiMDllNTE3MTA2OWY1M2ZiMjdkMDcwYzEwN2MyMjgyNDcwMTcwMzY3YWRiNjc5YTNjMDlhMjczNTRiOWQ5OWYxNWQzMyIsImlhdCI6MTc3NTY2NDY0NSwibmJmIjoxNzc1NjY0NjQ1LCJleHAiOjE3ODAxODU2MDAsInN1YiI6IjEyNjQyMzE0IiwiZ3JhbnRfdHlwZSI6IiIsImFjY291bnRfaWQiOjMyNDk1NjI2LCJiYXNlX2RvbWFpbiI6ImFtb2NybS5ydSIsInZlcnNpb24iOjIsInNjb3BlcyI6WyJwdXNoX25vdGlmaWNhdGlvbnMiLCJmaWxlcyIsImNybSIsImZpbGVzX2RlbGV0ZSIsIm5vdGlmaWNhdGlvbnMiXSwiaGFzaF91dWlkIjoiMGFiYmNhYWUtYzUzNi00NzMwLWEyODAtNDdjNjAwMjBkMGJmIiwiYXBpX2RvbWFpbiI6ImFwaS1iLmFtb2NybS5ydSJ9.GWXm7Z4v0eXQcb9w3SrlTdeY31GGzJf4Ta7EZZune6O61j520bu-rQKyLeUhJNjZz4tPDLBbf7l5_P-60QGAg_aW-JwCwExpMb04_0FLZIl4GcclB_dnT9zoccvsrXe58bNl50du4hiAhan58GWb51K9zM0BI8A0cOB9Hasytno1dJ_eLF9euyYlP5d_yRqV-5TsryUCg6PDwQaKgfZsjOh-bHY60vog6NHuM5u66BN6l_4XI44tTARWYDySRx9UpwnsQLkCgDUfPKHE9ij4D92-hJJSkUubn9jygblVasxoOz0oCH65Rx38Jo2wHuZ21RjgGXOp-Dwb5gBOW3zoxA',
            'results_field_id' => 1902565,
            'results_history_field_id' => 1908251,
        ],
    ],
    'upload' => [
        'max_photo_size' => (int)(getenv('MAX_PHOTO_SIZE') ?: 5242880), // 5MB
        'allowed_extensions' => ['jpg', 'jpeg', 'png', 'gif', 'webp'],
        'upload_dir' => __DIR__ . '/uploads'
    ],
    'logging' => [
        'enabled' => true,
        'level' => getenv('LOG_LEVEL') ?: 'WARNING', // DEBUG, INFO, WARNING, ERROR
        'log_dir' => __DIR__ . '/logs',
        'max_file_size' => 10485760, // 10MB
        'rotation_count' => 5
    ],
    'security' => [
        'enable_cors' => true,
        'cors_origins' => ['*'], // Установите конкретные домены в production
        'enable_rate_limiting' => true,
        'rate_limit_requests' => 100,
        'rate_limit_period' => 3600 // секунды
    ]
];

// Способ 2: Через файл .env.local (альтернатива)
$env_file = __DIR__ . '/.env.local';
if (file_exists($env_file)) {
    $env_lines = file($env_file, FILE_IGNORE_NEW_LINES | FILE_SKIP_EMPTY_LINES);
    foreach ($env_lines as $line) {
        if (strpos($line, '=') !== false && strpos($line, '#') !== 0) {
            list($key, $value) = explode('=', $line, 2);
            putenv(trim($key) . '=' . trim($value, '\'"'));
        }
    }
}

// Способ 3: Прямое указание (НЕ РЕКОМЕНДУЕТСЯ для production)
// $config = [
//     'api' => [
//         'subdomain' => 'your_subdomain',
//         'auth_key' => 'your_auth_key',
//         'base_url' => 'https://your_subdomain.t8s.ru/Api/V2'
//     ],
//     ...
// ];

// Валидация конфигурации
function validate_config() {
    if (empty($GLOBALS['config']['api']['subdomain']) || 
        $GLOBALS['config']['api']['subdomain'] === 'your_subdomain') {
        throw new Exception('Ошибка конфигурации: установите HOLLYHOP_SUBDOMAIN');
    }
    
    if (empty($GLOBALS['config']['api']['auth_key']) || 
        $GLOBALS['config']['api']['auth_key'] === 'your_auth_key') {
        throw new Exception('Ошибка конфигурации: установите HOLLYHOP_AUTH_KEY');
    }
}

// Получение конфигурации
function get_config($key = null) {
    global $config;
    
    if ($key === null) {
        return $config;
    }
    
    $keys = explode('.', $key);
    $value = $config;
    
    foreach ($keys as $k) {
        if (!isset($value[$k])) {
            return null;
        }
        $value = $value[$k];
    }
    
    return $value;
}

// Инициализация директорий
function init_directories() {
    $log_dir = get_config('logging.log_dir');
    $upload_dir = get_config('upload.upload_dir');
    
    foreach ([$log_dir, $upload_dir] as $dir) {
        if (!is_dir($dir)) {
            mkdir($dir, 0755, true);
        }
    }
}

return $config;
?>
