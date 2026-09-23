<?php
require_once('./logger.php');
require_once('./amo_func.php');
require_once __DIR__ . '/zoom_salesbot_reminders.php';

// Любой входящий webhook является тиком планировщика просроченных агентов.
// Запуск идемпотентный: JSON-агент блокируется и после успеха получает статус sent.
try {
    $agentResult = zoom_salesbot_run_agents();
    if (($agentResult['sent'] ?? 0) > 0 || ($agentResult['errors'] ?? 0) > 0) {
        log_message('INFO: Агенты Zoom обработаны в sourceHook', $agentResult, 'sourceHook.php');
    }
} catch (Throwable $exception) {
    log_message('ERROR: Ошибка запуска агентов Zoom в sourceHook', ['error' => $exception->getMessage()], 'sourceHook.php');
}

$amoWorkerMode = null;
$amoWorkerPayloadFile = null;
if (PHP_SAPI === 'cli' && isset($argv) && is_array($argv)) {
    foreach ($argv as $argument) {
        if (str_starts_with($argument, '--amo-worker=')) {
            $amoWorkerMode = substr($argument, strlen('--amo-worker='));
        }
        if (str_starts_with($argument, '--amo-payload=')) {
            $amoWorkerPayloadFile = substr($argument, strlen('--amo-payload='));
        }
    }

    if ($amoWorkerPayloadFile !== null && is_file($amoWorkerPayloadFile)) {
        $workerPayload = json_decode((string) file_get_contents($amoWorkerPayloadFile), true);
        if (is_array($workerPayload)) {
            $_POST = $workerPayload;
        }
        @unlink($amoWorkerPayloadFile);
    }
}

function forward_update_webhook_to_index(array $webhook_data, int $leadId, string $reason): void
{
    $targetUrl = 'https://srm.chinatutor.ru/index.php';
    $postBody = http_build_query($webhook_data);

    $ch = curl_init();
    curl_setopt($ch, CURLOPT_URL, $targetUrl);
    curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
    curl_setopt($ch, CURLOPT_POST, true);
    curl_setopt($ch, CURLOPT_POSTFIELDS, $postBody);
    curl_setopt($ch, CURLOPT_TIMEOUT, 20);
    curl_setopt($ch, CURLOPT_HTTPHEADER, [
        'Content-Type: application/x-www-form-urlencoded'
    ]);

    $response = curl_exec($ch);
    $httpCode = (int) curl_getinfo($ch, CURLINFO_HTTP_CODE);
    $curlError = curl_error($ch);
    curl_close($ch);

    if ($curlError !== '') {
        log_message('ERROR: Не удалось пробросить update webhook в index.php', [
            'lead_id' => $leadId,
            'reason' => $reason,
            'curl_error' => $curlError,
        ], 'sourceHook.php');
        return;
    }

    log_message('INFO: Update webhook проброшен в index.php', [
        'lead_id' => $leadId,
        'reason' => $reason,
        'http_code' => $httpCode,
        'response_preview' => mb_substr((string) $response, 0, 500),
    ], 'sourceHook.php');
}

// --- Конфигурация ---
$salebot_api_key = '35fa1d3f223b1be010e3f95bf0fc5e44';
$subdomain = 'directorchinatutorru';
$target_field_id = 1598377; // ID поля "Канал"

$enum_map = [
    'Instagram' => 7557063,  // insta SB
    'VK' => 7557061,         // vk SB
    'Telegram' => 7557065,   // TG SB
    'WhatsApp' => 7557067,   // WA SB
    'Онлайн-чат' => 7557069, // Онлайн-чат
    'MAX' => 7557059         // max SB
];

// Маппинг client_type для тех же значений
$client_type_map = [
    '10' => 7557063, // Instagram -> insta SB
    '0' => 7557061,  // VK -> vk SB
    '1' => 7557065,  // Telegram -> TG SB
    '6' => 7557067,  // WhatsApp -> WA SB
    '4' => 7557059,  // Viber -> max SB (нет отдельного)
    '8' => 7557059,  // Facebook -> max SB (нет отдельного)
    '13' => 7557059  // Телефония -> max SB (нет отдельного)
];

// ДАННЫЕ ПРИХОДЯТ В $_POST
$webhook_data = $_POST;

if (empty($webhook_data)) {
    log_message('INFO: sourceHook получен без данных, пропускаем', [], 'sourceHook.php');
    acknowledgeAmoWebhook();
    exit;
}

// Получаем ID сделки
$leadId = null;

if (isset($webhook_data["leads"]["update"][0]["id"])) {
    $leadId = (int) $webhook_data["leads"]["update"][0]["id"];
}

if (!$leadId) {
    log_message('INFO: sourceHook без lead_id, пропускаем', [
        'keys' => array_keys($webhook_data),
    ], 'sourceHook.php');
    acknowledgeAmoWebhook();
    exit;
}

log_message('INFO: sourceHook получен', [
    'lead_id' => $leadId,
    'has_name' => isset($webhook_data["leads"]["update"][0]["name"]),
    'event_keys' => isset($webhook_data['leads']) && is_array($webhook_data['leads']) ? array_keys($webhook_data['leads']) : [],
], 'sourceHook.php');

if (!claimWebhookCooldown($leadId, 30)) {
    acknowledgeAmoWebhook();
    exit;
}

if ($amoWorkerMode === null) {
    log_message('INFO: sourceHook будет обработан в текущем процессе после быстрого ответа', [
        'lead_id' => $leadId,
    ], 'sourceHook.php');
}

acknowledgeAmoWebhook();

// Получаем название сделки
$lead_name = $webhook_data["leads"]["update"][0]["name"] ?? '';

// --- Извлекаем client_id из названия сделки ---
preg_match('/№(\d+)/', $lead_name, $matches);

if (empty($matches)) {
    log_message('ERROR: Не удалось найти client_id в названии сделки: ' . $lead_name, [], 'sourceHook.php');
    forward_update_webhook_to_index($webhook_data, $leadId, 'client_id_not_found');
    exit;
}

$salebot_client_id = $matches[1];

// --- ПОЛУЧАЕМ ТЕКУЩЕЕ ЗНАЧЕНИЕ ПОЛЯ "КАНАЛ" ---
$current_enum_id = null;

try {
    $lead_info = get($subdomain, "/api/v4/leads/{$leadId}", $GLOBALS['data']);
    
    if (isset($lead_info['custom_fields_values']) && is_array($lead_info['custom_fields_values'])) {
        foreach ($lead_info['custom_fields_values'] as $field) {
            if (($field['field_id'] ?? 0) == $target_field_id) {
                if (isset($field['values'][0]['enum_id'])) {
                    $current_enum_id = $field['values'][0]['enum_id'];
                }
                break;
            }
        }
    }
    
    if (!$current_enum_id) {
    }
    
} catch (Exception $e) {
    log_message('ERROR: Ошибка при получении данных сделки', [
        'error' => $e->getMessage(),
        'lead_id' => $leadId
    ], 'sourceHook.php');
}

// --- Запрашиваем переменные клиента из Salebot ---
$salebot_url = "https://chatter.salebot.pro/api/{$salebot_api_key}/get_variables?client_id={$salebot_client_id}";


$ch = curl_init();
curl_setopt($ch, CURLOPT_URL, $salebot_url);
curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
curl_setopt($ch, CURLOPT_TIMEOUT, 10);

$salebot_response = curl_exec($ch);
$http_code = curl_getinfo($ch, CURLINFO_HTTP_CODE);

if (curl_error($ch)) {
    log_message('ERROR: Ошибка cURL при запросе к Salebot: ' . curl_error($ch), [], 'sourceHook.php');
    curl_close($ch);
    forward_update_webhook_to_index($webhook_data, $leadId, 'salebot_curl_error');
    exit;
}

curl_close($ch);


if ($http_code != 200) {
    log_message('ERROR: Salebot вернул ошибку HTTP ' . $http_code, [], 'sourceHook.php');
    forward_update_webhook_to_index($webhook_data, $leadId, 'salebot_http_error');
    exit;
}

// Декодируем ответ Salebot
$salebot_data = json_decode($salebot_response, true);

if (!$salebot_data) {
    log_message('ERROR: Не удалось декодировать ответ Salebot', ['response' => $salebot_response], 'sourceHook.php');
    forward_update_webhook_to_index($webhook_data, $leadId, 'salebot_invalid_json');
    exit;
}

// --- ОПРЕДЕЛЯЕМ НОВОЕ ЗНАЧЕНИЕ ИЗ SALEBOT ---
$new_enum_id = null;

// Сначала пробуем получить messenger
if (isset($salebot_data['messenger']) && !empty($salebot_data['messenger'])) {
    $messenger_value = $salebot_data['messenger'];
    $new_enum_id = $enum_map[$messenger_value] ?? null;
}

// Если не нашли по messenger, пробуем по client_type
if (!$new_enum_id && isset($salebot_data['client_type'])) {
    $client_type = (string)$salebot_data['client_type'];
    $new_enum_id = $client_type_map[$client_type] ?? null;
}

if (!$new_enum_id) {
    log_message('WARNING: Не удалось определить канал, ставим max SB', [
        'messenger' => $salebot_data['messenger'] ?? 'не указан',
        'client_type' => $salebot_data['client_type'] ?? 'не указан'
    ], 'sourceHook.php');
    
    $new_enum_id = 7557059; // max SB по умолчанию
}



// --- ПРОВЕРЯЕМ, НУЖНО ЛИ ОБНОВЛЯТЬ ---
if ($current_enum_id === $new_enum_id) {
    forward_update_webhook_to_index($webhook_data, $leadId, 'channel_already_actual');
    exit;
}

// --- Обновляем сделку ---
$leadsData = [
    'id' => $leadId,
    'custom_fields_values' => [
        [
            'field_id' => $target_field_id,
            'values' => [
                [
                    'enum_id' => $new_enum_id
                ]
            ]
        ]
    ]
];


try {
    $result = post_or_patch(
        $subdomain,
        $leadsData,
        "/api/v4/leads/{$leadId}",
        $GLOBALS['data'],
        'PATCH'
    );
    
    log_message('SUCCESS: Поле "Канал" обновлено', [
        'lead_id' => $leadId,
        'enum_id' => $new_enum_id,
    ], 'sourceHook.php');
    
} catch (Exception $e) {
    log_message('ERROR: Ошибка при обновлении', [
        'error' => $e->getMessage(),
        'lead_id' => $leadId
    ], 'sourceHook.php');
    forward_update_webhook_to_index($webhook_data, $leadId, 'channel_update_failed');
}

function acknowledgeAmoWebhook(): void
{
    http_response_code(200);
    header('Content-Type: text/plain; charset=utf-8');
    header('Connection: close');
    echo "OK";

    if (function_exists('fastcgi_finish_request')) {
        fastcgi_finish_request();
        return;
    }

    if (ob_get_level() > 0) {
        ob_end_flush();
    }
    flush();
}

function claimWebhookCooldown(int $leadId, int $seconds): bool
{
    $lockDir = __DIR__ . '/locks';
    if (!is_dir($lockDir)) {
        @mkdir($lockDir, 0755, true);
    }

    $stateFile = $lockDir . "/source_webhook_{$leadId}.state";
    $now = time();
    $lastRun = is_file($stateFile) ? (int) trim((string) @file_get_contents($stateFile)) : 0;

    if ($lastRun > 0 && ($now - $lastRun) < $seconds) {
        log_message('INFO: sourceHook пропущен по cooldown', [
            'lead_id' => $leadId,
            'seconds' => $seconds,
            'last_run_at' => date('c', $lastRun),
        ], 'sourceHook.php');
        return false;
    }

    @file_put_contents($stateFile, (string) $now, LOCK_EX);
    return true;
}
?>
